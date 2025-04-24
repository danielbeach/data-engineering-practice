import os.path
import requests
import os
from zipfile import ZipFile, BadZipFile
import shutil

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def downloadFile():
    folderPath = 'Exercises/Exercise-1/downloads'
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
        print("Create downloads folder successfully.")
    else :
        print("Folder has existed.")
    for url in download_uris:
        filePath = os.path.join(folderPath, os.path.basename(url))
        getFileDownload = requests.get(url)
        if getFileDownload.status_code == 200:
            with open(filePath, 'wb') as f:
                f.write(getFileDownload.content)
                print(f'Write {os.path.basename(url)} successfully.')
        else :
            print("Cannot connect.")

def extractFile():
    for url in download_uris:
        filePath = os.path.join('Exercises/Exercise-1/downloads', os.path.basename(url))
        try: 
            with ZipFile(filePath, 'r') as zip_ref:
                zip_ref.extractall('Exercises/Exercise-1/downloads')
            os.remove(filePath)
        except BadZipFile:
            print(f"Cannot extract {filePath}")
        except Exception as e:
            print(e)
    if os.path.exists('Exercises/Exercise-1/downloads/__MACOSX'):
        shutil.rmtree('Exercises/Exercise-1/downloads/__MACOSX')

def main():
    print("Starting downloadFile function")
    downloadFile()
    print("Starting extractFile function")
    extractFile()

if __name__ == "__main__":
    main()