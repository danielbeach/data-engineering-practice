import requests
import os
import zipfile
import shutil
from urllib.request import urlretrieve

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def check_url_validity(url):
    """Check if the URL is valid by sending a Head request."""
    try:
        response = requests.head(url, allow_redirects=True)
        if response.status_code == 200:
            return True
        else:
            str_log = f"URL not valid: {url} (Status Code: {response.status_code})"
            with open("log_file.txt", "a") as f:
                f.write(str_log + "\n")
            print(str_log)
            return False
    except Exception as e:
        print(f"Error checking URL: {url} - {e}")
        return False


def download_and_unzip(url, file_name, download_dir):
    """Download and unzip files from URLs"""
    # Save zip file
    urlretrieve(url, f"{download_dir}/{file_name}")

    # unzip the file
    with zipfile.ZipFile(f"{download_dir}/{file_name}", "r") as zip_ref:
        zip_ref.extractall(download_dir)

    # Delete the zip file after the extraction
    os.remove(f"{download_dir}/{file_name}")
    # Remove the __MACOSX directory if it exists
    remove_macosx_dir(download_dir)


def remove_macosx_dir(directory):
    """Remove all __MACOSX directories and their contents recursively"""
    for root, dirs, files in os.walk(directory):
        if "__MACOSX" in dirs:
            macosx_path = os.path.join(root, "__MACOSX")
            print("Removing {macosx_path} directory...")
            shutil.rmtree(macosx_path)
            print("Removed {macosx_path}")


def main():
    # Ensure the 'Downloads' directory exists
    if not os.path.exists("Downloads"):
        os.makedirs("Downloads")

    download_dir = "Downloads"

    # List of files names
    list_files = [i.split("/")[-1] for i in download_uris]

    # Download data files
    for url, file_name in zip(download_uris, list_files):
        if check_url_validity(url):
            download_and_unzip(url, file_name, download_dir)
        else:
            print(f"Skipping download for {file_name} due invalid URL.")


if __name__ == "__main__":
    main()
