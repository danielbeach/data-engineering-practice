import boto3
from botocore.config import Config
import gzip


'''
- bucket common crawl
- path: crawl-data/CC-MAIN-2022-05/wet/paths.gz
    WET stands for "WARC Encapsulated Text"
    WET format is quite simple: the WARC metadata contains various details,
    including the URL and the length of the plaintext data,
    with the plaintext data following immediately afterwards.
- AWS services (e.g EMR) support the s3:// protocol,
    and you may directly specify your input as s3://commoncrawl/path_to_file
- region: us-east-1

EC:
    - no disk write -> context wrap the file being read in...
    - can stream using a generator and next method
    use tarfile module
        use tarfile.open(mode='r:gz') as tf:
            tf.extractfile(member) -> Extract a member from the archive as a file object.

current worktime: 77min+45+45+25+10+
'''

BUCKET = 'commoncrawl'
COMMON_CRAWL_KEY = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
NEW_FILE_NAME = 'wet.paths.gz'

def main():
    ''' docstring '''
    global COMMON_CRAWL_KEY

    session = boto3.session.Session()

    esssthree = session.client('s3')
    # esssthree = session.client('s3', aws_access_key_id='', aws_secret_access_key='')

    esssthree.download_file(BUCKET, COMMON_CRAWL_KEY, NEW_FILE_NAME)

    with gzip.open(filename=NEW_FILE_NAME, mode='rt') as paths_file:  # default reading mode = 'rb'
        COMMON_CRAWL_KEY = paths_file.readline()
        print(f'new common crawl s3 key: {COMMON_CRAWL_KEY}')

    # print(f'New URI to get: {COMMON_CRAWL_KEY}')

    return


if __name__ == "__main__":
    main()
