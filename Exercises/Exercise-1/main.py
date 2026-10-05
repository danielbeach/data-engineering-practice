# import requests
import asyncio
import aiohttp
import os
import zipfile
import concurrent.futures
import shutil

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

os.makedirs("downloads", exist_ok=True)
DIR = os.path.abspath("downloads")

for file in os.listdir(DIR):
    rem_file = os.path.join(DIR, file)
    try:
        if os.path.isfile(file) or os.path.islink(file):
            os.unlink(file)
        elif os.path.isdir(file):
            shutil.rmtree(file)
    except Exception as e:
        print('Failed to delete %s. Reason: %s' % (file, e))

async def zip_downloads(uri):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(uri) as response:
                print("Status: ", response.status)

                if response.status == 404:
                    print("URI not found")
                    return
                
                try:
                    st = os.path.basename(uri)
                    file_path = os.path.join(DIR, st)
                    
                    with open(file_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                    
                    with zipfile.ZipFile(file_path, 'r') as zFile:
                        zFile.extractall(path=DIR)
                    print(f"Success while extracting: {st}")
                    os.remove(file_path)

                except Exception as e:
                    print(f"Error processing {uri}: {e}")

        except Exception as e:
            print(f"Error requesting {uri}: {e}")

async def main():
    # for downloading files, is better to use a simple async function, instead of complex thread management,
    # which is useful for heavy CPU computations.
    tasks = [zip_downloads(uri) for uri in download_uris]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
