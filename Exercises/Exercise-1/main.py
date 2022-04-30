import requests
import os
from zipfile import ZipFile


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
    # your code here
    for uri in download_uris:
        response = requests.get(uri, stream=True)

        if response.ok:
            zip_folder = uri.split('/')[-1]
            # file_name = os.path.splitext(zip_folder)[0]

            current_directory = os.getcwd()
            download_dir = 'downloads'
            dest_path = os.path.join(current_directory, download_dir)
            zip_path = os.path.join(dest_path, zip_folder)
            
            if not os.path.exists(dest_path):
                os.mkdir(dest_path)

            with open(zip_path, 'wb') as output_file:
                output_file.write(response.content)
            print(f"Download completed: {zip_folder}")

            with ZipFile(zip_path, 'r') as zip_file:
                zip_file.extractall(dest_path)
                os.remove(zip_path)
            print(f"Finished extracting zip file: {zip_folder}")

            rubbish_dir = os.path.join(dest_path, '__MACOSX')
            if os.path.exists(rubbish_dir):
                os.rmdir(rubbish_dir)
        else:
            print(f"This URL is not valid: {uri}")


if __name__ == '__main__':
    main()
