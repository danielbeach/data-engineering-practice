import requests
import tempfile
import zipfile
import os
import logging
import time

# Create and configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler('dumb.log')
stream_handler = logging.StreamHandler()

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def iterate_uris(_path):
    for uri in download_uris:
        try:
            start_time = time.time()
            response = requests.request(method='GET', url=uri, timeout=30)
            end_time = time.time()
            duration = end_time - start_time

            if duration > 30:
                logger.warning(f"Request to {uri} took longer than the timeout value. Duration: {duration} seconds")
        except Exception as e:
            logger.exception(e)
            pass

        if response.status_code == 200:
            filename = uri.split("/")[-1]
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_file_path = os.path.join(temp_dir, filename)
                with open(temp_file_path, "wb") as f:
                    logger.info(f"This is the location of tempDir: {f}")
                    f.write(response.content)
                    logger.info(f'successfully wrote the response content!')

                with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                    zip_ref.extractall(os.path.join(_path, f"{filename}.csv"))
                    logger.info("successfully extracted the response's content to a file")
        else:
            logger.exception(f"response wasn't successful! here's the status code: {response.status_code}")

def main():
    path = os.path.join(os.getcwd(), 'downloads')
    path = os.path.normpath(path)
    if not os.path.exists(path):
        os.mkdir(path=path)
        logger.info("created a downloads folder!")
        iterate_uris(path)
    else:
        logger.info("such a path exists!")
        iterate_uris(path)


if __name__ == "__main__":
    main()
