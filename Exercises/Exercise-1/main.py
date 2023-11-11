import requests
import zipfile
import os

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    for uri in download_uris:
        process_uri(uri)

def process_uri(uri):
    try:
        filename = os.path.basename(uri)
        response = requests.get(uri)
        print('processing file : ' + filename)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
                with zipfile.ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall('downloads')
            os.remove(filename)
        else:
            print("Failed downloading zip file")
    except Exception as e:
        print(f"Encountered exception: {str(e)}")

if __name__ == "__main__":
    main()
