import pytest
from main import download_files
from aioresponses import aioresponses
from concurrent.futures import ThreadPoolExecutor
import aiohttp

@pytest.mark.asyncio
async def test_download_files_success(tmp_path):
    """Test successful download with mocked response"""
    # Arrange
    download_folder=tmp_path 
    fake_url="http://fakeurl.com/testfile.csv"
    #Example of data
    fake_data= (
        "Name,Last modified,Size,Description\n"
        "01002099999.csv,2024-01-19 15:45,178821,\n"
        "01368099999.csv,2024-01-19 15:45,475362,\n"
        "03761099999.csv,2024-01-19 15:45,11077920,\n"
    ).encode("utf-8")
    

    #Mock GET request
    with aioresponses() as mocked:
        mocked.get(fake_url, status=200, body=fake_data)

        #Act
        async with aiohttp.ClientSession() as session:
            with ThreadPoolExecutor(max_workers=3) as executor:
                results = await download_files(session, executor, fake_url, download_folder)

    # Assert
    assert results is not None
    assert download_folder.joinpath(results).exists() == True 


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,body,should_succeed",
    [
        (
            200,
            (
                "Name,Last modified,Size,Description\n"
                "01002099999.csv,2024-01-19 15:45,178821,\n"
                "01368099999.csv,2024-01-19 15:45,475362,\n"
                "03761099999.csv,2024-01-19 15:45,11077920,\n"
            ).encode("utf-8"),
            True,
        ),
        (404, b"Not Found", False),
        (500, b"Internal Server Error", False),
    ],
)
async def test_download_files(tmp_path, status, body, should_succeed):
    """Parametrized test for download_files with aiohttp + aioresponses."""

    # Arrange
    download_folder=tmp_path 
    fake_url="http://fakeurl.com/testfile.csv"
    

    #Mock GET request
    with aioresponses() as mocked:
        mocked.get(fake_url, status=status, body=body)

        #Act
        async with aiohttp.ClientSession() as session:
            with ThreadPoolExecutor(max_workers=3) as executor:
                results = await download_files(session, executor, fake_url, download_folder)

    # Assert
    if should_succeed:
        assert results is not None
        assert download_folder.joinpath(results).exists() == should_succeed 
    else:
        assert results is None or tmp_path+'/'+results.exists() == should_succeed