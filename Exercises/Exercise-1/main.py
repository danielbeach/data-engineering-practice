import os
import tempfile
import concurrent.futures
import time
import zipfile
import requests
import datetime

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
    file_path = "/logs/logs.txt"
    timestamp = str(datetime.datetime.now())
    if os.path.exists(file_path):
        with open(file_path, "a") as file:
            file.write(timestamp + "\n")
    else:
        with open(file_path, "w") as file:
            file.write("First lina.\n")

    time.sleep(60)
    max_threads = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(download_and_unzip, uri) for uri in download_uris]
        try:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                print(result)
        except Exception as e:
            print(e)


def download_and_unzip(uri: str, to='downloads') -> list[str]:
    os.makedirs(to, exist_ok=True)
    temp_dir = tempfile.mkdtemp()

    r = requests.get(uri)
    r.raise_for_status()
    filename = os.path.join(temp_dir, os.path.basename(uri))
    with open(filename, 'wb') as file:
        file.write(r.content)
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(to)
    return zip_ref.namelist()


if __name__ == "__main__":
    main()
