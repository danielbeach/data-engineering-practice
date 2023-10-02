import boto3
import tempfile
import gzip
import json
import os
from botocore import UNSIGNED
from botocore.client import Config

def main():
    aws_access_key = os.environ.get("aws_access_key")
    aws_secret_key = os.environ.get("aws_secret_key")
    # with open("KEYS.json") as config_file:
    #     config = json.load(config_file)
    #
    # aws_access_key = config.get("aws_access_key")
    # aws_secret_key = config.get("aws_secret_key")
    #
    # if aws_access_key is None:
    #     raise ValueError("SECRET_KEY not found in the configuration file.")
    # if aws_secret_key is None:
    #     raise ValueError("SECRET_KEY not found in the configuration file.")

    bucket_name = 'commoncrawl'
    file_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

    get_file_from_uri(get_boto3_client(aws_access_key, aws_secret_key), bucket_name,
                      get_first_uri(get_boto3_client(aws_access_key, aws_secret_key), bucket_name, file_key))


def get_boto3_client(aws_access_key_id, aws_secret_access_key):
    print("creating client...")
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')
    return s3


def get_first_uri(s3, bucket_name, file_key):

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        gzipped_content = response['Body'].read()
        new_key = gzip.decompress(gzipped_content).decode('utf-8').split('\n', 1)[0].strip()
        return new_key

    except Exception as e:
        raise e


def get_file_from_uri(s3, bucket_name, file_key):

    temp_folder = tempfile.TemporaryDirectory()

    try:
        local_file_path = temp_folder.name + '/' + file_key.split('/')[-1]
        s3.download_file(bucket_name, file_key, local_file_path)
        with gzip.open(local_file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                print(line.strip())

    except Exception as e:
        raise e


if __name__ == "__main__":
    main()
