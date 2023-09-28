import requests
import zipfile
import os

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    for uri in download_uris:
        download_and_unzip(uri)


def download_and_unzip(uri: str, to='downloads'):
    os.makedirs(to, exist_ok=True)
    r = requests.get(uri)
    if r:
        with open('test.zip', 'wb') as f:
            f.write(r.content)
        with zipfile.ZipFile('test.zip', 'r') as zip_ref:
            zip_ref.extractall(to)
        os.remove('test.zip')


if __name__ == "__main__":
    main()
