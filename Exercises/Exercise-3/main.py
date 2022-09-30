import boto3
import gzip 

def main():
    #
    s3 = boto3.client(
        's3', 
        aws_access_key_id = 'AKIASWLAIMNBHWSFVNWU', 
        aws_secret_access_key = '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'
        )
    file = s3.download_file('commoncrawl', 'crawl-data/CC-MAIN-2022-05/wet.paths.gz', 'myfile')
    text = gzip.open('myfile', 'rb').read()
    uri = text.decode().split("\n")[0]

    file2 = s3.download_file('commoncrawl', 'crawl-data/CC-MAIN-2022-05/segments/1642320299852.23/wet/CC-MAIN-20220116093137-20220116123137-00000.warc.wet.gz', 'myfile2')
    text2 = gzip.open('myfile2', 'rb').read()
    print(text2.decode())

if __name__ == '__main__':
    main()
