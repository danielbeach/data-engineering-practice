import requests
import os 
import zipfile
from urllib.error import URLError

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


def create_new_folder(new_folder_name: str) -> None:
    if not os.path.exists(new_folder_name):    
        os.mkdir(new_folder_name)
    else:
        print('"Downloads" folder already exists.')


def name_uri_file(uri: str) -> str:
    return uri.split('/')[-1]


def main() -> None:
    """
    Downloads and extract zip files from 'download_uris' to 'downloads' folder 
    then deletes zip files
    """
    # create folder if doesn't already exists
    create_new_folder('downloads')

    for uri in download_uris:

        # download zip file 
        req = requests.get(uri)

        # check that uri is reachable
        if req.status_code != 200:
            print(f"uri {uri} is unreachable.")
            continue

        # save zip file in 'downloads' folder
        with open(f"downloads/{name_uri_file(uri)}", 'wb') as myfile:
            myfile.write(req.content)
        
        # extract zip file if extracted csv file does not already exists
        if not os.path.isfile(f"downloads/{name_uri_file(uri)[:-3]}csv"):
            print(f"Extracting {name_uri_file(uri)[:-3]}csv...")
            with zipfile.ZipFile(f"downloads/{name_uri_file(uri)}", 'r') as zip:
                zip.extractall("downloads")
        else:
            print(f"downloads/{name_uri_file(uri)[:-3]}csv already exists.")

        # delete zip file
        os.remove(f"downloads/{name_uri_file(uri)}") 

if __name__ == '__main__':
    main()
