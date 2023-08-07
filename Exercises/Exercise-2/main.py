import pandas as pd
import requests
import pandas
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin

url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'


# find the file based on textinside the <a> tag.<a href="01001099999.csv">01011099999.csv</a>
def find_the_file():
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find the table element
    table = soup.find('table')
    # list - Find all rows within the table except the first two rows and the last row
    rows = table.find_all('tr')[2:-1]

    # print(len(rows)) -> 13546 rows
    timeslot_value = '2022-02-07 14:03'
    for row in rows:
        # print(row)
        # Find all <td> elements in the current row
        cols = row.find_all('td')
        if len(cols) >= 4 and cols[1].text.strip() == timeslot_value:
            target_file = cols[0].text
            print(row)
            print(f"The target file is {target_file}")
            # td_element = target_row.find('td')
            # a_element = td_element.find('a')
            return target_file, row


def file_download(file, row):
    td_element = target_row.find('td')
    a_element = td_element.find('a')
    file_url = urljoin(url, a_element['href'])
    response = requests.get(file_url)
    with open(target_file, 'wb') as f:
        f.write(response.content)


def read_file(file):
    df = pd.read_csv(file)
    max_value = df['HourlyDryBulbTemperature'].max()
    print(max_value)
    rows_with_max_value = df[df['HourlyDryBulbTemperature'] == max_value]
    print(rows_with_max_value.to_dict('records'))


if __name__ == "__main__":
    target_file, target_row = find_the_file()
    file_download(target_file, target_row)
    read_file(target_file)
