import requests
import sys 
from pathlib import Path
import zipfile 
import os
import aiohttp
import asyncio
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

cwd = Path.cwd()
folder_path = cwd / "downloads" 
folder_path.mkdir(parents=True,exist_ok=True)

def sync_download():
    # cwd = Path.cwd()
    # folder_path = cwd / "downloads" 
    # folder_path.mkdir(parents=True,exist_ok=True)

    for uri in download_uris:
       
       file_name = uri.split('/')[-1]
       file_path = folder_path / file_name
       response = requests.get(uri)
       if response.status_code==200:
        with open(file_path,"wb") as f:
            f.write(response.content)
        csv_file = file_name.split(".")[0] + ".csv"
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extract(csv_file,folder_path)
        os.remove(file_path)

def save_unzip_clean(file_path,csv_file, data):
    with open(file_path, 'wb') as f:
        f.write(data)
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extract(csv_file,folder_path)
    os.remove(file_path)

async def async_download(session,executor,uri):

    file_name = uri.split('/')[-1]
    file_path = folder_path / file_name
    csv_file = file_name.split(".")[0] + ".csv"

    #async with aiohttp.ClientSession() as session:
    async with session.get(uri) as response:
        if response.status==200:
            data = await response.read()
            #save_unzip_clean(file_path,csv_file, data)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, save_unzip_clean,file_path,csv_file, data)
            
            # with open(file_path, 'wb') as f:
            #     f.write(data)
            # with zipfile.ZipFile(file_path, "r") as zip_ref:
            #     zip_ref.extract(csv_file,folder_path)
            # os.remove(file_path)
    

async def main():
    # your code here
    #pass
    #sync_download()
    executor = ThreadPoolExecutor(max_workers=5)
    async with aiohttp.ClientSession() as session:
        tasks= [async_download(session,executor,uri) for uri in download_uris]
        await asyncio.gather(*tasks)
    executor.shutdown(wait=True)



if __name__ == "__main__":
    #sinc_main()
    asyncio.run(main())
