'''
Generally, your script should do the following ...

+create the directory downloads if it doesn't exist
+download the files one by one.
+split out the filename from the uri, so the file keeps its original filename.
+Each file is a zip, extract the csv from the zip and delete the zip file.

For extra credit, download the files in an async manner using the Python package aiohttp. 
Also try using ThreadPoolExecutor in Python to download the files. 
Also write unit tests to improve your skills.

Download URIs are listed in the main.py file.

Hints
Don't assume all the uri's are valid.
One approach would be the Python method split() to retrieve filename for uri, or maybe find the last occurrence of / and take the rest of the string.
'''

import requests
import os
from zipfile import ZipFile, BadZipFile

from pathlib import Path

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
    cwd = Path(os.getcwd())
    #print(cwd)
    files_dir = Path(f"{cwd}\downloads") # search keyword: f string
    try:
        os.mkdir(files_dir) 
        print(f"Folder {cwd}\downloads has been created.")
    except FileExistsError as e:
        print("Folder exists. Continuing...")

    os.chdir(files_dir)
    cwd = Path(os.getcwd())
#    print(cwd)
    
#    for uri in download_uris:
#        with open(f"{uri.split('/')[-1]}", "wb") as f:
#            f.write(requests.get(uri, allow_redirects=True).content)

    for file in os.listdir(cwd):
        file_dir = cwd / file
        try:
            with ZipFile(file_dir, 'r') as zObject:
                zObject.extractall(path=cwd)
                print("File extracted.")
                input("Waiting for user input")
        except BadZipFile as e:
            pass

        os.remove(file)
        print("File removed.")


if __name__ == "__main__":
    main()