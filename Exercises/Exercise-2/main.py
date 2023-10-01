import os
import tempfile
import requests
import pandas
from bs4 import BeautifulSoup

uri = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'


def main():
    return_value(uri, search_for_link(uri))


def search_for_link(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        tr_element = soup.find_all('tr')
        for i in tr_element:
            if '2022-02-07 14:03' in str(i):
                download_link = i.find('a').get('href')
                return download_link
        raise FileNotFoundError


def return_value(url, download_link):
    temp_dir = tempfile.mkdtemp()
    r = requests.get(url + download_link)
    r.raise_for_status()
    filename = os.path.join(temp_dir, os.path.basename(url + download_link))
    with open(filename, 'wb') as file:
        file.write(r.content)
        df = pandas.read_csv(filename)
        value = df.at[df['HourlyDryBulbTemperature'].idxmax(), 'HourlyDryBulbTemperature']
        print(value)


if __name__ == "__main__":
    main()
