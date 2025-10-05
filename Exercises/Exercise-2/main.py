import requests
from bs4 import BeautifulSoup
import pandas as pd
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pandas as pd

URL='https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
DOWNLOAD_FOLDER = Path.cwd() / "downloads" 
DOWNLOAD_FOLDER.mkdir(parents=True,exist_ok=True)

def parse_html(html:str) -> list:
    """
    Get html string as input and parse it to html
    Retrive table rows from html table element and return a list
    """
    #parse HTML
    soup = BeautifulSoup(html,'html.parser')

    #locate the table
    table = soup.find('table')

    #locate the table rows inside the table
    table_rows = table.find_all('tr')
    
    #extract data from table cells
    data = []
    for row in table_rows:
        cols = row.find_all(['td','th']) #find both data and header cells
        row_data = [col.get_text(strip=True) for col in cols]
        #if "2024-01-19 15:45" in str(row_data):
        data.append(row_data)
    
    return data

def build_url(data:list) -> list:
    """
    Get a list, create pandas DataFrame, get max date to filter dataframe
    Retrive file names to build url
    Return urls list
    """
    #extract header
    header = data[0]

    #extrart data
    data_body = data[1:]

    #create dataframe
    df = pd.DataFrame(data_body,columns=header)

    #convert the column with datetime string to datetime data type
    df['Last modified']=pd.to_datetime(df['Last modified'])

    #get the max date of modifications
    max_date = df['Last modified'].max()

    #filter rows with max_date
    df_max_date = df[df['Last modified']==max_date]

    #convert Name column values into list
    files_list=df_max_date['Name'].to_list()

    #create list of urls with files names
    urls_list=[]
    for file in files_list:
        url = URL + file
        urls_list.append(url)
    
    return urls_list


def save_file(folder_path:str,data) -> bool:
    """
    save file and return True if file was created
    """
    with open(folder_path, 'wb') as f:
        f.write(data)
        #writer = csv.writer(f)
        #writer.writerows(data)
    print(f'Saved csv file in:{folder_path}')
    if folder_path.exists():
        return True
    else:
        False

async def download_files(session,executor,url,download_folder):
    """
    Download file async
    Return True if successful, False otherwise
    """
    
    #retrive file name
    file_name = url.split('/')[-1]
    #create folder path to save file
    folder_path = download_folder / file_name

    async with session.get(url) as response:
        if response.status==200:
            print("Page fetched successfully!")
            data = await response.read()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, save_file,folder_path,data)
            return file_name
        



async def fetch_and_save(url:str,output_file:str) -> bool:
    """
    Fetch a webpage and save its content into a text file.
    Returns True if successful, False otherwise.
    """
    #send GET request
    response=requests.get(url)

    if response.status_code==200:
        print("Page fetched successfully!")

        #parse HTML
        data=parse_html(response.text)
        
        #check if there is data
        if len(data) > 1:
            #build urls list
            urls = build_url(data)

            #start async and parallel downloads of files
            executor = ThreadPoolExecutor(max_workers=7)
            async with aiohttp.ClientSession() as session:
                tasks= [download_files(session,executor,url,DOWNLOAD_FOLDER) for url in urls]
                results=await asyncio.gather(*tasks)
            executor.shutdown(wait=True)

            #save the urls into text file
            with open(output_file,"w",encoding="utf-8") as f:
                #f.write(response.text)
                for url in urls:
                    f.writelines(str(url)+'\n')

                print(f"Content saved to {output_file}")
        return results
    
    else:
        print(f"Fail to fetch page. Status code:{response.status_code}")
        return False

def retrive_highest_value(download_folder,files:list,search_column:str,num_files=1,num_rows=5):##TO-DO:terminar de revisar funcionamiento 
    """
    Get a list of csv files. Create a pandas DataFrame.
    Search the highest value of the column provided.
    Print records in terminal
    """

    for file in files[0:num_files]:
        #file=files[0:6]
        
        #read csv file
        df = pd.read_csv(download_folder / file)
        
        #drop null values
        df = df.dropna(subset=[search_column])

        #extract digits if the value mix number + text and convert to float
        if df[search_column].dtype == 'object':
            df[search_column] = df[search_column].str.extract(r'(\d+)').astype(float)
        else:
            df[search_column] = df[search_column].astype(float)
        #get the max value
        max_value = df[search_column].max()

        #filter rows with max_value
        df_max_value = df[df[search_column]==max_value]
        
        #get the first five rows to be printed in the terminal
        first_five=df_max_value.head(n=num_rows)
        first_five=first_five[['STATION','DATE','LATITUDE','LONGITUDE','HourlyDryBulbTemperature']]
        
        for index, row in first_five.iterrows():
            print(f'Row {index}: {row.to_dict()}')
        




async def main():
    files = await fetch_and_save(URL,"page_content.csv")
    retrive_highest_value(DOWNLOAD_FOLDER,files,'HourlyDryBulbTemperature',len(files),1)

if __name__ == "__main__":
    asyncio.run(main())
