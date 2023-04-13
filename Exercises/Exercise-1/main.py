import requests
import zipfile
import os

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


def main():

    PATH = "Exercise-1/downloads"

    try:
        os.makedirs(PATH)
    except FileExistsError:
        pass

    for uri in download_uris:
        filename = uri.split('/')[-1]        
        
        r = requests.get(uri, allow_redirects=True)
        if r.status_code == 200:
            open(f"{PATH}/{filename}", 'wb').write(r.content)
    
    for zip in os.listdir(PATH):
        with zipfile.ZipFile(f"{PATH}/{zip}") as zip_ref:
            zip_ref.extractall(PATH)

    for file in os.listdir(PATH):
        if file.endswith(".zip"):
            os.remove(f"{PATH}/{file}")

if __name__ == '__main__':
    main()
