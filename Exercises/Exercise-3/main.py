import boto3
import gzip
import io

bucket_name = "commoncrawl"
file_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

s3 = boto3.client("s3")

# Tải và đọc file .gz trong RAM
response = s3.get_object(Bucket=bucket_name, Key=file_key)
compressed_data = response["Body"].read()
with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
    first_line = gz.readline().decode("utf-8").strip()

# Tải file WET từ dòng đầu tiên
response2 = s3.get_object(Bucket=bucket_name, Key=first_line)

# Đọc từng dòng và in ra
with gzip.GzipFile(fileobj=response2["Body"]) as f:
    for line in f:
        print(line.decode("utf-8").strip())