import requests
import gzip
import io
import sys

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import os
        os.environ['PYTHONIOENCODING'] = 'utf-8'

BASE_URL = "https://data.commoncrawl.org/"

def download_and_extract_gz_in_memory(url):
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        print(f"Successfully downloaded {url}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download {url}: {e}")

    compressed_data = io.BytesIO(response.content)
    try:
        with gzip.GzipFile(fileobj=compressed_data, mode='rb') as gz:
            return io.BytesIO(gz.read())
    except gzip.BadGzipFile as e:
        raise Exception(f"Failed to decompress {url}: {e}")

def stream_gz_lines(url):
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        print(f"Streaming {url}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to stream {url}: {e}")

    with io.BytesIO() as buffer:
        for chunk in response.iter_content(chunk_size=8192):
            buffer.write(chunk)
            buffer.seek(0)
            try:
                with gzip.GzipFile(fileobj=buffer, mode='rb') as gz:
                    cnt = 0
                    while cnt <= 10000:
                        line = gz.readline()
                        if not line:
                            break
                        yield line.decode('utf-8', errors='replace').rstrip('\n')
                        cnt += 1
            except (EOFError, gzip.BadGzipFile):
                continue
            buffer.seek(0)
            buffer.truncate()

def main():
    wet_paths_url = BASE_URL + "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    print(f"Attempting to download {wet_paths_url}...")
    try:
        extracted_data = download_and_extract_gz_in_memory(wet_paths_url)
    except Exception as e:
        print(f"Error downloading wet.paths.gz: {e}")
        return

    extracted_data.seek(0)
    first_line = extracted_data.readline().decode('utf-8', errors='replace').strip()
    if not first_line:
        print("Error: No URI found in wet.paths.gz")
        return
    print(f"First URI: {first_line}")

    wet_file_url = BASE_URL + first_line
    print(f"Downloading and streaming {wet_file_url}...")

    try:
        for line in stream_gz_lines(wet_file_url):
            try:
                print(line)
            except UnicodeEncodeError:
                print(line.encode('utf-8', errors='replace').decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"Error streaming WET file: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")