import io
import requests
import pandas
from bs4 import BeautifulSoup

uri = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'


def main():
    print(get_max_temp(uri, get_filename(uri)))


def get_filename(url, last_modified='2022-02-07 14:03'):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    tr_element = soup.find_all('tr')
    for i in tr_element:
        if last_modified in str(i):
            filename = i.find('a').get('href')
            return filename
    raise FileNotFoundError


def get_max_temp(url, filename):
    r = requests.get(url + filename)
    r.raise_for_status()

    df = pandas.read_csv(io.StringIO(r.text))
    return df['HourlyDryBulbTemperature'].max()


if __name__ == "__main__":
    main()
