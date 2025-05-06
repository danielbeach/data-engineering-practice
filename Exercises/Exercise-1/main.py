import os
import requests
import zipfile
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Danh sách các URL để tải xuống
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # URL lỗi
]

# Thư mục lưu tệp tải xuống
download_dir = "downloads"

# Hàm tạo thư mục downloads
def create_download_dir():
    os.makedirs(download_dir, exist_ok=True)

# Hàm tải tệp đồng bộ
def download_file(url):
    try:
        filename = os.path.basename(url)
        filepath = os.path.join(download_dir, filename)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Đã tải xong: {filename}")
        return filepath
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

# Hàm giải nén tệp ZIP
def extract_zip(filepath):
    try:
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(download_dir)
        os.remove(filepath)
        print(f"Đã giải nén và xóa: {filepath}")
    except zipfile.BadZipFile:
        print(f"Tệp không hợp lệ: {filepath}")

# Hàm thực thi đồng bộ
def sync_download_and_extract():
    create_download_dir()
    for url in download_uris:
        filepath = download_file(url)
        if filepath:
            extract_zip(filepath)

# Hàm tải tệp bất đồng bộ
async def async_download_file(url):
    filename = os.path.basename(url)
    filepath = os.path.join(download_dir, filename)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(filepath, "wb") as f:
                        while chunk := await response.content.read(1024):
                            f.write(chunk)
                    print(f"Đã tải xong: {filename}")
                    return filepath
                else:
                    print(f"Lỗi HTTP {response.status} cho URL: {url}")
                    return None
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

# Hàm bất đồng bộ tải và giải nén
async def async_download_and_extract():
    create_download_dir()
    tasks = [async_download_file(url) for url in download_uris]
    filepaths = await asyncio.gather(*tasks)
    for filepath in filepaths:
        if filepath:
            extract_zip(filepath)

# Hàm tải và giải nén đa luồng
def threaded_download_and_extract():
    create_download_dir()
    with ThreadPoolExecutor() as executor:
        filepaths = list(executor.map(download_file, download_uris))
        for filepath in filepaths:
            if filepath:
                extract_zip(filepath)

# Điểm bắt đầu chương trình
def main():
    # Lựa chọn phương pháp thực thi
    print("Thực hiện đồng bộ ...")
    sync_download_and_extract()
    # asyncio.run(async_download_and_extract())  # Bỏ comment để chạy bất đồng bộ
    # threaded_download_and_extract()  # Bỏ comment để chạy đa luồng

if __name__ == "__main__":
    main()