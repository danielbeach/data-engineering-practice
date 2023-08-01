import requests
import os
import shutil


# from zipfile import ZipFile


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
    open("downloads/" + local_filename, 'wb').write(response.content)
    print(f"{local_filename} successfully downloaded.")
    try:
        shutil.unpack_archive("downloads/" + local_filename, "downloads")
        # remove the zip folder after unzip
    except Exception as ex:
        print("error occurred!", ex)
    finally:
        # if any errors in unzipping step, the next commnad won't run. so run here anyways.
        os.remove("downloads/" + local_filename)

# loading the temp.zip and creating a zip object


if __name__ == "__main__":
    # 1.create folder if not existing
    folder_existing("downloads")

    # 2.download files
    for i in range(0, len(download_uris)):
        download_unzip_remove(download_uris[i])
