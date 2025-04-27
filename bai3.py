import boto3
import gzip
import io
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_and_process_s3_file(bucket_name, file_key):
    """
    Download a file from S3 and return its content as a byte stream
    """
    try:
        # Initialize boto3 S3 client
        s3_client = boto3.client('s3')
        logger.info(f"Downloading s3://{bucket_name}/{file_key}")
        
        # Download the file directly to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        
        logger.info(f"Successfully downloaded {len(file_content)} bytes")
        return file_content
    
    except Exception as e:
        logger.error(f"Error downloading file from S3: {e}")
        raise

def extract_first_uri_from_gzipped_content(gzipped_content):
    """
    Extract the first line/URI from gzipped content
    """
    try:
        # Decompress the gzipped content
        with gzip.GzipFile(fileobj=io.BytesIO(gzipped_content), mode='rb') as f:
            # Read just the first line
            first_line = f.readline().decode('utf-8').strip()
        
        logger.info(f"Extracted URI from first line: {first_line}")
        return first_line
    
    except Exception as e:
        logger.error(f"Error extracting URI from gzipped content: {e}")
        raise

def stream_s3_file_to_stdout(bucket_name, file_key):
    """
    Stream a file from S3 line by line to stdout
    """
    try:
        # Initialize boto3 S3 client
        s3_client = boto3.client('s3')
        logger.info(f"Streaming s3://{bucket_name}/{file_key}")
        
        # Get the file object
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # Check if file is gzipped
        if file_key.endswith('.gz'):
            # Stream and decompress the file
            buffer = io.BytesIO(response['Body'].read())
            with gzip.GzipFile(fileobj=buffer, mode='rb') as f:
                for line in f:
                    # Print each line to stdout
                    sys.stdout.write(line.decode('utf-8'))
        else:
            # Stream the file directly
            for line in response['Body'].iter_lines():
                # Print each line to stdout
                sys.stdout.write(line.decode('utf-8') + '\n')
        
        logger.info("Finished streaming file to stdout")
    
    except Exception as e:
        logger.error(f"Error streaming file from S3: {e}")
        raise

def parse_s3_uri(uri):
    """
    Parse an S3 URI into bucket name and key
    Format: s3://bucket-name/path/to/file
    """
    if not uri.startswith('s3://'):
        raise ValueError(f"Invalid S3 URI format: {uri}")
    
    # Remove the 's3://' prefix
    path = uri[5:]
    
    # Split into bucket name and key
    parts = path.split('/', 1)
    bucket_name = parts[0]
    file_key = parts[1] if len(parts) > 1 else ''
    
    return bucket_name, file_key

def main():
    try:
        # Step 1: Download the wet.paths.gz file from S3
        original_bucket = 'commoncrawl'
        original_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
        
        logger.info("Starting the process...")
        gzipped_content = download_and_process_s3_file(original_bucket, original_key)
        
        # Step 2: Extract the first URI from the gzipped content
        first_uri = extract_first_uri_from_gzipped_content(gzipped_content)
        
        # Step 3: Parse the URI into bucket and key components
        # The URI might be in the format: crawl-data/CC-MAIN-2022-05/segments/.../warc.wet.gz
        # We need to handle both full S3 URIs and relative paths
        if first_uri.startswith('s3://'):
            second_bucket, second_key = parse_s3_uri(first_uri)
        else:
            # If it's a relative path, assume it's in the same bucket
            second_bucket = original_bucket
            second_key = first_uri
        
        # Step 4: Stream the second file to stdout
        stream_s3_file_to_stdout(second_bucket, second_key)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()