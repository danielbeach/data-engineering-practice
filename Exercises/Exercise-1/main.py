import requests
import os   #library to navigate directory and file structure
from zipfile import ZipFile


download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip'
]


def get_file_name(url):
    first = url.rsplit("/")[1]
    file_name = first.split(".")[0]
    return file_name


def main():
    directory = "downloads"
    os.mkdir(directory)
    os.chdir(directory)

    for url in download_uris:
        response = requests.get(url)

        filename = get_file_name(url)

        open(filename+'.zip', 'wb').write(response.content)

        with ZipFile(filename+'.zip', 'r') as zipobj: # read zipfile, zipobj is the zipfile
            list_of_file_names = zipobj.namelist()   #list of files in zipfiles
            for file in list_of_file_names:         #loop through zipfile
                if file.endswith('.csv'):           #checks if there are files that end with the file extension ".csv"
                    zipobj.extract(file)

        if os.path.exists(filename+'.zip'):
            os.remove(filename+'.zip')
                        

if __name__ == '__main__':
    main()
