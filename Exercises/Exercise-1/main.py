import requests
import concurrent.futures
import os
from pathlib import Path
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def create_directory(directory: Path):
    print("Creating directory: %s" % directory)
    # create parent directories if necessary, if directory exists does not raise exception
    directory.mkdir(parents=True, exist_ok=True)
    print('Created directory "%s".' % directory)
        

def download_file(url, filepath):
    print("Accessing URL: %s" % url)
    response = requests.get(url)
    if response.status_code == 200:      
        with open(filepath, 'wb') as file:
            file.write(response.content)
        print("Saved file as '%s'." % filepath)
        return True
    else:
        print("Failed to download. Status code: %s." % response.status_code)
        return False

def unzip_file(filepath, dirpath):
    try:
        with zipfile.ZipFile(filepath, 'r') as zipf:
            zipf.extractall(dirpath)
    except Exception as e:
        print("Error when unzipping file:", e)
    

def delete_archive(filepath):
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
        else:
            print("The file does not exist")
    except Exception as e:
        print("Error when deleting file:", e)
        
def main():
    DIRPATH = Path("./downloads")
    # make a directory if it does not already exist
    create_directory(DIRPATH)
    
    # Solution using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(download_uris)) as executor:
        futures = {executor.submit(download_file, url, DIRPATH / os.path.basename(url)) : url for url in download_uris}
        
        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            try:
                download_successful = future.result
                if download_successful: 
                    unzip_file(DIRPATH / os.path.basename(url), DIRPATH)
                    delete_archive(DIRPATH / os.path.basename(url))
                else:
                    print("Skipping to next file.")
            except Exception as e:
                print("Encountered exception:", e)

if __name__ == "__main__":
    main()
