import pytest
import asyncio
from pathlib import Path
from aioresponses import aioresponses
import csv
import zipfile

from downloader import async_download


def create_test_zip(zip_path: Path, csv_filename: str):
    # Create the CSV content in memory
    csv_data = [
        ["id", "name", "age"],
        [1, "Alice", 30],
        [2, "Bob", 25],
    ]

    # Write CSV to a temporary file before zipping
    temp_csv = zip_path.parent / csv_filename
    with open(temp_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_data)

    # Create ZIP and add the CSV file
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(temp_csv, arcname=csv_filename)

    # Remove the temporary CSV (optional)
    temp_csv.unlink()



TEST_URL = "https://example.com/test.zip"
BAD_URL = "https://example.com/404.zip"


@pytest.mark.asyncio
async def test_download_success(tmp_path):
    """Test successful download with mocked response"""
    # Arrange
    fake_data = create_test_zip(tmp_path, "test.csv")
    test_file = tmp_path / "test.zip"

    # Override OUTPUT_DIR for test isolation
    folder_path_test=tmp_path / "downloads"
    folder_path_test.mkdir(exist_ok=True)

    with aioresponses() as mocked:
        mocked.get(TEST_URL, status=200, body=fake_data)

        # Act
        results = await async_download([TEST_URL],folder_path_test)

    # Assert
    downloaded_file = results[0]
    assert downloaded_file.exists()
    assert downloaded_file.read_bytes() == fake_data



@pytest.mark.asyncio
async def test_download_success(tmp_path):
    """Test successful download with mocked response"""
    # Arrange
    folder_path_test=tmp_path / "downloads"
    folder_path_test.mkdir(exist_ok=True)
    fake_data = create_test_zip(folder_path_test / "test.zip","test.csv")
    #(folder_path / "test.zip").unlink(missing_ok=True)

    with aioresponses() as mocked:
        mocked.get(TEST_URL, status=200, body=fake_data)

        # Act
        results = await async_download([TEST_URL],folder_path_test)

    # Assert
    downloaded_file = results[0]
    assert downloaded_file.exists()
    assert downloaded_file.read_bytes() == fake_data

@pytest.mark.asyncio
async def test_download_failure(tmp_path):
    """Test failed download (404)"""
    with aioresponses() as mocked:
        mocked.get(BAD_URL, status=404)

        results = await async_download([BAD_URL],folder_path)

    # The result should be None because of failure
    assert results[0] is None