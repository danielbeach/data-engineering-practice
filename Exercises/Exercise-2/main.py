import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

uri = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
timestamp = "2022-02-07 14:03"


# get website content from link
def pull_website_content(link):
    r = requests.get(link)
    return r.text


# search for website link
def search_for_content(page, search_time):
    soup = BeautifulSoup(page, "html.parser")
    file_list = soup.find(name="table").find_all(name="tr")
    files = []

    # search for file using timestamp, if it matches add it to a list
    for file in file_list:
        # print(file.find_all_next(name="td"))
        if search_time in file.getText():
            text = file.getText()
            text = text.split("2022")[0]
            text = uri + text
            files.append(text)
    # print(files)
    return files


# download all csv data from the website
def download_csv(url):
    # create a download directory for files
    if "Downloads" not in os.listdir():
        os.mkdir("Downloads")

    # download the files concurrently
    def download(files):
        file_name = files.split("/")[-1]
        with requests.get(files, stream=True) as r:
            with open(f"Downloads/{file_name}", "wb") as csv:
                csv.write(r.content)

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download, url)


# analyze the csv data for find_HourlyDryBulbTemperature
def analyze_CsvFile():
    files = os.listdir("Downloads")
    # print(files)

    # print the max hourly temperature and the row location
    def find_HourlyDryBulbTemperature(file):
        df = pd.read_csv(f"Downloads/{file}", low_memory=False)
        max_df = df["HourlyDryBulbTemperature"].max()
        max_row = df["HourlyDryBulbTemperature"].idxmax()
        print(f"The HourlyDryBulbTemperature for '{file}' is {max_df}, located at row {max_row} ")

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(find_HourlyDryBulbTemperature, files)


def main():

    content = pull_website_content(uri)

    files_link = search_for_content(content, timestamp)

    download_csv(files_link)

    analyze_CsvFile()


if __name__ == '__main__':
    main()
