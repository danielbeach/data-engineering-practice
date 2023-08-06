#useful link: https://realpython.com/python-download-file-from-url/

import requests
import os
import shutil

def folder_existing(path):
    # Check whether the specified path exists or not
    isExist = os.path.exists(path)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("The new directory is created!")


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def download_unzip_remove(url):
    print(f"processing url:{url}")
    local_filename = url.split('/')[-1]
    print(f"file name:{local_filename}")
    response = requests.get(url, allow_redirects=True)

    #save data retrieved from url to a local file by using response.content.
    #you’ll open a new file in binary mode for writing ('wb') and then write the downloaded content to this file
    open("downloads/" + local_filename, 'wb').write(response.content)
    print(f"{local_filename} successfully downloaded.")
    try:
        shutil.unpack_archive("downloads/" + local_filename, "downloads")
        # remove the zip folder after unzip
    except Exception as ex:
        print("error occurred!", ex)
    finally:
        # remove the zip folder after unzip
        os.remove("downloads/" + local_filename)

if __name__ == "__main__":
    # 1.create folder if not existing
    folder_existing("downloads")

    # 2.download files
    for i in range(0, len(download_uris)):
        download_unzip_remove(download_uris[i])
