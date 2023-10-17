import os,shutil
import glob
import zipfile
import requests
import io
import time
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


t1 = time.perf_counter()

def download_file(url):
    downloads = r'./downloads'
    if not os.path.exists(downloads):
        os.makedirs(downloads)

    r = requests.get(url)
    fileName = url.split("/")[-1]
    with open(fileName, "wb") as file:
    # create a folder downloads
        zip = zipfile.ZipFile(io.BytesIO(r.content))
        zip.extractall(downloads) # extract to folder downloads
    os.remove(fileName) # remove zip file

def remove_zip_files():
    # remove zip files
    for f in glob.glob("*.zip"):
        os.remove(f)


def remove_csv_files():
    # remove csv files
    try:
        shutil.rmtree('./downloads/')
    except FileNotFoundError or OSError:
        pass


def main():
    # your code here
    # create a folder downloads
    try:
        remove_csv_files()
        remove_zip_files()
        for url in download_uris:
            download_file(url)            
        pass
        
    except zipfile.BadZipFile or FileNotFoundError:
        pass
    

main()
t2 = time.perf_counter()
print(f'Finished Single threading in {t2-t1} seconds')

if __name__ == "__main__":
    t1 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(download_file, download_uris)
    t2 = time.perf_counter()
    print(f'Finished Multi threading in {t2-t1} seconds')
