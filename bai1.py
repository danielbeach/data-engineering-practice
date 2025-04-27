import os
import requests
import zipfile

# Danh sách các URL cần tải
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    
     # Lỗi URL
]

DOWNLOAD_FOLDER = "downloads"


def download_file(url, folder):
    try:
        filename = url.split("/")[-1]  # Lấy tên file từ url
        zip_path = os.path.join(folder, filename)

        print(f"Tải xuống: {filename}...")
        response = requests.get(url)
        response.raise_for_status()  # Kiểm tra lỗi HTTP

        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"Đã lưu: {zip_path}")

        # Giải nén
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(folder)
        print(f"Đã giải nén: {zip_path}")

        # Xóa file zip sau khi giải nén
        os.remove(zip_path)
        print(f"Đã xóa file zip: {zip_path}\n")

    except requests.exceptions.RequestException as e:
        print(f"❌ Lỗi khi tải {url}: {e}")
    except zipfile.BadZipFile:
        print(f"❌ File zip không hợp lệ: {zip_path}")


def main():
    # Tạo thư mục downloads nếu chưa tồn tại
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
        print(f"Đã tạo thư mục {DOWNLOAD_FOLDER}")

    # Tải và giải nén từng file
    for url in download_uris:
        download_file(url, DOWNLOAD_FOLDER)


if __name__ == "__main__":
    main()
