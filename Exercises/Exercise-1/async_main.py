import asyncio
import aiohttp
import os
import shutil


async def download_file(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if "content-disposition" in response.headers:
                header = response.headers["content-disposition"]
                filename = header.split("filename=")[1]
                print(f'if filename:{filename}')
            else:
                filename = url.split("/")[-1]
                print(f'else filename:{filename}')

            with open("downloads/" + filename, mode="wb") as file:
                while True:
                    chunk = await response.content.read()
                    if not chunk:
                        break
                    file.write(chunk)
                print(f"Downloaded file {filename}")


def folder_existing(path):
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)
        print("The new directory is created!")


def unzip_func(url):
    try:
        filename = url.split("/")[-1]
        shutil.unpack_archive("downloads/" + filename, "downloads")
        print(f'unzipped {filename} successfully!')
    except Exception as ex:
        print(f"error occurred! while unzipping {filename} ", ex)

# except FileNotFoundError as e:
#     print(f"Error: {e}")

async def process_files():
    # Unzip
    for i in range(0, len(download_uris)):
        unzip_func(download_uris[i])

    # Remove .zip files
    directory = "downloads"
    test = os.listdir(directory)
    for item in test:
        if item.endswith(".zip"):
            os.remove(os.path.join(directory, item))
            print(f'Removed {item} successfully!')
    ######await asyncio.gather(*tasks)
async def main():
    tasks = [download_file(url) for url in download_uris]
    await asyncio.gather(*tasks)


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

if __name__ == "__main__":
    # downloads path creation
    folder_existing("downloads")
    # Loop over list of urls and download all asynchronously
    asyncio.run(main())
    # Run the event loop again for unzipping and removing
    asyncio.run(process_files())
#By using asyncio.run(process_files()), you are running the event loop again and allowing
# the unzipping and removal operations to be performed asynchronously,
# which should avoid the "RuntimeError: Event loop is closed" issue.