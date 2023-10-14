import os
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor

uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    create_download_directory("downloads")
    asyncio.run(download_files())


def create_download_directory(directory_name: str):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)


async def download_files():
    executor = ThreadPoolExecutor(len(uris))
    tasks = executor.map(download_file, uris)
    # tasks = [download_file(url) for url in uris]
    await asyncio.gather(*tasks)


async def download_file(url: str):
    filename = url.split("/")[-1]
    if file_year_is_valid(filename):
        print(f"Downloading {filename}...")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                assert response.status == 200
                zip_content = await response.read()
                unzip_files(zip_content)


def file_year_is_valid(filename: str):
    return int(filename.split("_")[2]) <= datetime.today().year


def unzip_files(zip_content: bytes):
    in_memory_zip_file = BytesIO(zip_content)
    with ZipFile(in_memory_zip_file) as zippy:
        for file in zippy.infolist():
            if file.filename.endswith(".csv") and not file.filename.startswith(
                "__MACOSX"
            ):
                with zippy.open(file.filename) as input_file:
                    with open(f"downloads/{file.filename}", "wb") as output_file:
                        for row in input_file:
                            output_file.write(row)


if __name__ == "__main__":
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Total time: {end - start} to process files")
