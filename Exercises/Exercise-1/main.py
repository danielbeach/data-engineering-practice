import requests
from pathlib import Path
import os
import concurrent.futures
import zipfile
import glob

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


# create the downloads directory
def create_directory():
    if "Downloads" not in os.listdir():
        p = Path("Downloads/")
        p.mkdir()


# download the zip file
def download_files():
    if len(os.listdir("Downloads")) < 7:
        try:
            os.removedirs("Downloads")
        except OSError as e:
            print("directory does not exist")
            create_directory()
        else:
            create_directory()

        def download_zip(download_uri):

            zip_name = download_uri.split("/")[-1]
            response = requests.get(download_uri)
            with open(f"Downloads/{zip_name}", "wb") as f:
                f.write(response.content)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(download_zip, download_uris)
    else:
        print("Files already exist")


# extract each files
def extract_file():
    zip_files = os.listdir("Downloads/")
    if len(zip_files) >= 6:
        def extract(file):
            d_zip = zipfile.ZipFile(f"Downloads/{file}", "r")
            d_zip.extractall(path="Downloads/")
            d_zip.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(extract, zip_files)


# delete the extracted zip file
def drop_zipfiles():
    for zip_file in glob.glob("Downloads/*.zip"):
        os.remove(zip_file)
        # print(zip_file)


def main():
    # create directory
    create_directory()

    # download files
    download_files()

    # extract the files
    extract_file()

    # delete zip files after extraction
    drop_zipfiles()


if __name__ == '__main__':
    main()
