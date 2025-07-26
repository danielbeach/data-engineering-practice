import requests
import sys 
from pathlib import Path
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
    # your code here
    #pass
    cwd = Path.cwd()
    folder_path = cwd / "downloads" 
    folder_path.mkdir(parents=True,exist_ok=True)

    for uri in download_uris:
       
       file_name = uri.split('/')[-1]
       file_path = folder_path / file_name
       response = requests.get(uri)
       if response.status_code==200:
        with open(file_path,"wb") as f:
            f.write(response.content)
        csv_file = file_name.split(".")[0] + ".csv"
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extract(csv_file,folder_path)
        os.remove(file_path)


if __name__ == "__main__":
    main()
