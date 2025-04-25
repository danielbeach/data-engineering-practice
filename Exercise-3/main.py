import requests
import gzip
import io

def download_file_from_s3_http(key: str) -> bytes:
    url = f"https://data.commoncrawl.org/{key}"  # Đảm bảo URL đúng
    print(f"Downloading from: {url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.content

def save_data_to_file(data: str, filename: str):
    with open(filename, 'a', encoding='utf-8') as file:  # Mở file ở chế độ thêm (append)
        file.write(data + "\n")

def main():
    print("Starting the process...")
    gz_key = 'crawl-data/CC-MAIN-2023-50/wet.paths.gz'
    gz_data = download_file_from_s3_http(gz_key)
    print("File downloaded successfully!")

    with gzip.GzipFile(fileobj=io.BytesIO(gz_data)) as gz_file:
        first_line = gz_file.readline().decode('utf-8').strip()
        print(f"First WET file key: {first_line}")

    wet_data = download_file_from_s3_http(first_line)
    print("Second file downloaded successfully!")

    with gzip.GzipFile(fileobj=io.BytesIO(wet_data)) as wet_file:
        # Lưu 50 dòng đầu tiên vào file
        for i, line in enumerate(wet_file):
            decoded_line = line.decode('utf-8').rstrip()
            print(decoded_line)  # In ra màn hình
            save_data_to_file(decoded_line, 'output.txt')  # Lưu vào tệp 'output.txt'
            if i > 50:  # Giới hạn in ra và lưu 50 dòng đầu tiên
                break

    print("Process completed!")

if __name__ == '__main__':
    main()