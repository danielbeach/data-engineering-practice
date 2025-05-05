import os
import requests
from zipfile import ZipFile, BadZipFile
import shutil

class Bai1:
    def __init__(self, download_uris, base_folder="Exercises/Exercise-1/downloads"):
        self.download_uris = download_uris
        self.base_path = os.getcwd()
        self.download_folder = os.path.join(self.base_path, base_folder)

    def create_download_folder(self):
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
            print("Created downloads folder successfully.")
        else:
            print("Folder already exists.")

    def download_files(self):
        for url in self.download_uris:
            file_path = os.path.join(self.download_folder, os.path.basename(url))
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                        print(f'File {os.path.basename(url)} downloaded successfully.')
                else:
                    print(f"Failed to download {url}. Status code: {response.status_code}")
            except Exception as e:
                print(f"Error downloading {url}: {e}")

    def extract_files(self):
        for url in self.download_uris:
            file_path = os.path.join(self.download_folder, os.path.basename(url))
            try:
                with ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(self.download_folder)
                os.remove(file_path)
                print(f"Extracted and removed {os.path.basename(file_path)}")
            except BadZipFile:
                print(f"Cannot extract {file_path} - Bad zip file")
            except Exception as e:
                print(f"Error extracting {file_path}: {e}")

        # Remove __MACOSX if it exists
        macosx_path = os.path.join(self.download_folder, "__MACOSX")
        if os.path.exists(macosx_path):
            shutil.rmtree(macosx_path)
            print("Removed __MACOSX folder.")

    def process(self):
        print("Creating download folder...")
        self.create_download_folder()
        print("Downloading files...")
        self.download_files()
        print("Extracting files...")
        self.extract_files()

if __name__ == "__main__":
    urls = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # invalid URL for testing
    ]
    manager = Bai1(urls)
    manager.process()