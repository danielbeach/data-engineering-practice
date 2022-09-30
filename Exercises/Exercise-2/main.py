import requests
import pandas as pd
from urllib.error import URLError
from bs4 import BeautifulSoup

url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
find_time = '2022-02-07 14:03  '

def main():
    """
    Finds a file updated at 'find_time' on website with 'url',
    downloads the file and prints records with highest HourlyDryBulbTemperature.
    """
    website = requests.get(url)
    if website.status_code != 200:
        raise URLError(f"Url {url} is not reachable.") 

    # Find 'find_time' 's first occurrence on website
    website_parse = BeautifulSoup(website.text, 'html.parser')
    time_tag = website_parse.find(string = find_time)

    # Go to its parent tag to find its corresponding file name
    time_desc = time_tag.find_parents('tr')
    file_text = time_desc[0].find('a').string
    
    # Find value in file 
    file_url = url + file_text
    file_content = pd.read_csv(file_url)
    max_value = file_content.loc[:,'HourlyDryBulbTemperature'].max() 
    print(file_content.loc[file_content['HourlyDryBulbTemperature'] == max_value])
    
if __name__ == '__main__':
    main()
