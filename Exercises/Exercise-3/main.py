import boto3
import gzip

def main():
    # your code here
    BUCKET_NAME = "commoncrawl"
    OBJECT_NAME = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    s3 = boto3.client('s3')

    wet_object = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)
    wet_content = wet_object['Body'].read()
    decompressed_wet = gzip.decompress(wet_content).decode("utf-8") 
    
    warc_key = decompressed_wet.partition('\n')[0]
    warc_object = s3.get_object(Bucket=BUCKET_NAME, Key=warc_key)
    warc_content = warc_object['Body'].read()
    decompressed_warc = gzip.decompress(warc_content).decode("utf-8")

    print(decompressed_warc)

if __name__ == '__main__':
    main()
