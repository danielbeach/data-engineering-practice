import requests
import regex as re
import aiohttp
import asyncio
import time
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]
async def async_generator_urls(urls):
    for url in urls:
        yield url

def main():
    for uri in download_uris:
        filename=re.findall("\/[a-zA-Z0-9_]+\.zip",uri)[0]
        filename=re.sub("/","Downloads/",filename)
        print(filename)
        response=requests.get(uri,filename,stream=True)
        handle = open(filename, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:   
                handle.write(chunk)

async def asyncmain():
    async with aiohttp.ClientSession() as session:
        async for uri in async_generator_urls(download_uris):
            async with session.get(uri) as resp:
                filename=re.findall("\/[a-zA-Z0-9_]+\.zip",uri)[0]
                filename=re.sub("/","Downloads/",filename)
                print(filename)
                with open(filename, 'wb') as fd:
                    while True:
                        chunk= await  resp.content.read(512)
                        fd.write(chunk)
                        if not chunk:
                            break


if __name__ == "__main__":
    start = time.time()
    #main()
    asyncio.run(asyncmain())
    end = time.time()
    print(end - start)