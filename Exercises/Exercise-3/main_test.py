from unittest.mock import Mock, patch
import boto3
import gzip
import moto
from main import get_first_uri, get_file_from_uri
import pytest


@pytest.fixture
def s3_client():
    # Use the Moto library to mock the S3 service
    with moto.mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


def test_get_first_uri(s3_client):
    # Create a test S3 bucket and upload a gzipped file
    bucket_name = 'my-test-bucket'
    file_key = 'test.txt.gz'
    gzipped_content = gzip.compress(b'First URI: example.com\nSecond URI: example2.com')

    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=gzipped_content)

    # Call the get_first_uri function
    result = get_first_uri(s3_client, bucket_name, file_key)

    # Perform assertions
    assert result == 'First URI: example.com'


def test_get_first_uri_error():

    mock_s3_client = Mock()
    # Configure the mock S3 client to raise an exception when get_object is called
    mock_s3_client.get_object.side_effect = Exception("Simulated S3 Error")
    with pytest.raises(Exception) as e:
        get_first_uri(mock_s3_client, 'my-test-bucket', 'test.txt.gz')

    # Check if the function raised the expected exception
    assert str(e.value) == "Simulated S3 Error"


def test_get_file_from_uri(s3_client):
    # Create a test S3 bucket and upload a gzipped file
    bucket_name = 'my-test-bucket'
    file_key = 'test.txt.gz'
    gzipped_content = gzip.compress(b'First URI: exampl2e.com\nSecond URI: example2.com')

    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=gzipped_content)

    # Call the get_first_uri function
    result = get_file_from_uri(s3_client, bucket_name, file_key)

    # Perform assertions
    assert result == 'First URI: exampl2e.com'


def test_get_file_from_uri_error():
    # Create a mock S3 client
    mock_s3_client = Mock()

    # Simulate an error when calling s3.download_file
    mock_s3_client.download_file.side_effect = Exception("Simulated S3 Error")

    with pytest.raises(Exception) as e:
        get_file_from_uri(mock_s3_client, 'my-test-bucket', 'test.txt.gz')

    assert str(e.value) == "Simulated S3 Error"


if __name__ == '__main__':
    pytest.main()






