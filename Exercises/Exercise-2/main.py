import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def main():
    content = pull_content()
    file = find_latest_modified(content.text)
    download_file(file)
    df = find_highest_temperature(file="01001499999.csv")
    print(df.info)


def pull_content(stream_content=None):
    response = requests.get(url, stream=stream_content)
    assert response.status_code == 200
    return response


def find_latest_modified(content):
    soup = BeautifulSoup(content)
    table_rows = soup.findAll("tr")
    last_modified_rows = [
        {
            "csv_file": table_rows[4].findAll("td")[0].find("a").decode_contents(),
            "date": row.findAll("td")[1].decode_contents().strip(),
        }
        for row in table_rows[3:]
        if len(row.findAll("td")) > 1
    ]
    df = pd.DataFrame(last_modified_rows)
    specific_date = df[df["date"] == "2022-02-07 14:03"]
    return specific_date["csv_file"].iloc[0]


def download_file(file):
    create_download_directory("downloads")
    response = pull_content(url + file, True)
    with open(f"downloads/{file}", "wb") as output_file:
        output_file.write(response.content)


def create_download_directory(directory_name):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)


def find_highest_temperature(file):
    df = pd.read_csv(f"downloads/{file}")
    max_temperature = df["HourlyDryBulbTemperature"].max()
    return df[df["HourlyDryBulbTemperature"] == max_temperature]


if __name__ == "__main__":
    main()
