import os
import tempfile
import requests
import pandas
from bs4 import BeautifulSoup


def main():
    links = []
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        tr_element = soup.find_all('tr')
        for i in tr_element:
            if '2022-02-07 14:03' in str(i):
                download_link = i.find('a').get('href')
                links.append(download_link)

    temp_dir = tempfile.mkdtemp()
    r = requests.get(url + links[0])
    r.raise_for_status()
    filename = os.path.join(temp_dir, os.path.basename(url + links[0]))
    with open(filename, 'wb') as file:
        file.write(r.content)
        df = pandas.read_csv(filename)
        index_of_max_value = df['HourlyDryBulbTemperature'].idxmax()
        value = df.at[index_of_max_value, 'HourlyDryBulbTemperature']
        print(value)


if __name__ == "__main__":
    main()
