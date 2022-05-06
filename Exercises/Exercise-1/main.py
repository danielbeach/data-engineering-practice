import requests
import os
import zipfile
import aiohttp
import aiofiles
import asyncio

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


def get_files_sync():
    #create dir
    folder = "downloads"
    folder_path = "./" + folder
    
    if not os.path.exists(folder_path):
        print("Folder doesn't exist")
        os.makedirs(folder_path)

    #download files
    for url in download_uris:
        print("fetching url:" + url)
        r = requests.get(url)
        if not r.status_code == 200:
            print('URL: %s Status %s' % (url, r.status_code))
            continue
    
        #get filename
        filename = url.split('/')[-1]
        file_path = folder_path + "/" + filename

        #saving file
        f = open(file_path, "wb")
        f.write(r.content)
        f.close()

        #extract zipfile
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(folder_path)

        #delete the zip files
        os.remove(file_path)

async def get_files_async():
    #create dir
    folder = "downloads"
    folder_path = "./" + folder
    
    if not os.path.exists(folder_path):
        print("Folder doesn't exist")
        os.makedirs(folder_path)

    #download files
    
    for url in download_uris:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                print("fetching url:" + url)
                if not response.status == 200:
                    print('URL: %s Status %s' % (url, response.status))
                    continue
            
                #get filename
                filename = url.split('/')[-1]
                file_path = folder_path + "/" + filename

                #saving file
                f = await aiofiles.open(file_path, mode='wb')
                await f.write(await response.read())
                await f.close()

                #extract zipfile
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(folder_path)

                #delete the zip files
                os.remove(file_path)

def main():
    # your code here
    #get_files_sync()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_files_async())
    


if __name__ == '__main__':
    main()
