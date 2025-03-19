import gzip
import io

import boto3

from constants import BUCKET, COMMON_CRAWL_KEY


def read_from_s3(client, bucket, key, num_lines=-1, print_out=False):
    ''' docstring. '''
    ret_line = ''
    data_file = io.BytesIO()
    print('*'*75)
    print(f'bucket: {bucket}, key: {key}')
    print('*'*75)
    client.download_fileobj(bucket, key, data_file)
    data_file.seek(0)

    with gzip.open(filename=data_file, mode='rt', encoding='utf-8') as curr_file:
        while num_lines:
            content = curr_file.readline()
            if content:
                num_lines -= 1
                ret_line = content
                if print_out:
                    print(content)

    data_file.close()

    return ret_line


def main():
    ''' docstring'''
    global COMMON_CRAWL_KEY

    session = boto3.session.Session()
    esssthree = session.client('s3')

    new_key = read_from_s3(esssthree, BUCKET, COMMON_CRAWL_KEY, num_lines=1)
    _ = read_from_s3(client=esssthree, bucket=BUCKET, key=new_key, print_out=True)

    return


if __name__ == "__main__":
    main()
