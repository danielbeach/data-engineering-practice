import pytest
import asyncio
from pathlib import Path
from aioresponses import aioresponses

from downloader import async_download

OUTPUT_DIR = Path("downloads")

TEST_URL = "https://example.com/test.zip"
BAD_URL = "https://example.com/404.zip"


@pytest.mark.asyncio
async def test_download_success(tmp_path):
    """Test successful download with mocked response"""
    # Arrange
    fake_data = b"fake zip content"
    test_file = tmp_path / "test.zip"

    # Override OUTPUT_DIR for test isolation
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "test.zip").unlink(missing_ok=True)

    with aioresponses() as mocked:
        mocked.get(TEST_URL, status=200, body=fake_data)

        # Act
        results = await async_download([TEST_URL])

    # Assert
    downloaded_file = results[0]
    assert downloaded_file.exists()
    assert downloaded_file.read_bytes() == fake_data

@pytest.mark.asyncio
async def test_download_failure(tmp_path):
    """Test failed download (404)"""
    with aioresponses() as mocked:
        mocked.get(BAD_URL, status=404)

        results = await async_download([BAD_URL])

    # The result should be None because of failure
    assert results[0] is None