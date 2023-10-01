import os
import io
import unittest
import zipfile
import tempfile
import requests
import requests_mock
from main import download_and_unzip


@requests_mock.Mocker()
class TestDownloadAndUnzip(unittest.TestCase):
    txt_file_content = "Very important text file."
    txt_file_name = "nice.txt"

    def test_success_single_file(self, m):
        urls = ["http://test.com/nice.zip"]
        file = self.txt_file_name
        content = self.txt_file_content

        # make fake file bytes
        file_bytes = create_mock_zip(file, content)
        m.get(urls[0], content=file_bytes)

        # Save it to random temporary directory, it returns name of file from zip
        temp_dir = tempfile.mkdtemp()
        files = download_and_unzip(urls[0], temp_dir)
        self.assertEqual([file], files)

        # Correct downloaded file content
        path = os.path.join(temp_dir, file)
        with open(path, "r") as downloaded_file:
            downloaded_content = downloaded_file.read()
            self.assertEqual(content, str(downloaded_content))

        # There is nothing else in this download dir
        self.assertEqual([file], os.listdir(temp_dir))

    def test_404_error(self, m):
        urls = ["http://test.com/nice.zip"]
        m.get(urls[0], status_code=404)
        with self.assertRaises(requests.HTTPError) as context:
            temp_dir = tempfile.mkdtemp()
            download_and_unzip(urls[0], temp_dir)
            self.assertEqual(str(context.exception), "requests.exceptions.HTTPError: 404 Client Error:"
                                                     " None for url: http://test.com/nice.zip")

    def test_not_zip_file(self, m):
        urls = ["http://test.com/nice.zip"]

        m.get(urls[0], content=b'content')

        with self.assertRaises(zipfile.BadZipFile) as context:
            temp_dir = tempfile.mkdtemp()
            download_and_unzip(urls[0], temp_dir)
            self.assertEqual(str(context.exception), "zipfile.BadZipFile: File is not a zip file")


def create_mock_zip(name: str, content: str):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zipf:
        text_content = content
        with zipf.open(name, 'w') as file:
            file.write(text_content.encode('utf-8'))
    zip_data = zip_buffer.getvalue()
    zip_buffer.close()

    return zip_data


if __name__ == '__main__':
    unittest.main()
