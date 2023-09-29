import requests
import zipfile
import os
import tempfile
import concurrent.futures

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
    max_threads = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(download_and_unzip, uri) for uri in download_uris]

        concurrent.futures.wait(futures)


def download_and_unzip(uri: str, to='downloads'):
    os.makedirs(to, exist_ok=True)
    temp_dir = tempfile.mkdtemp()
    try:
        r = requests.get(uri)
        if r.status_code == 200:
            filename = os.path.join(temp_dir, os.path.basename(uri))
            with open(filename, 'wb') as file:
                file.write(r.content)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(to)
        else:
            print(f"Failed to download file from {uri}. Status code: {r.status_code}")
    except Exception as e:
        print(f"An error occurred while downloading {uri}: {str(e)}")


if __name__ == "__main__":
    main()
