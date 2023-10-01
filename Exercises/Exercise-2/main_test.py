import unittest
import requests
import requests_mock
from main import get_filename, get_max_temp


@requests_mock.Mocker()
class TestDownloadAndUnzip(unittest.TestCase):

    def test_string_not_found(self, m):
        urls = ["http://test.com/nice.zip"]
        m.get(urls[0], content=b'content')
        with self.assertRaises(FileNotFoundError) as context:
            get_filename(urls[0])
            self.assertEqual(str(context.exception), "FileNotFoundError")

    def test_get_link_error(self, m):
        urls = ["http://test.com/nice.zip"]
        m.get(urls[0], status_code=404)
        with self.assertRaises(requests.HTTPError) as context:
            get_filename(urls[0])
            self.assertEqual(str(context.exception), "requests.exceptions.HTTPError: 404 Client Error:")

    def test_get_max_temp(self, m):
        urls = ["http://example.com/data.csv"]
        csv_data = """Date,HourlyDryBulbTemperature
        2023-10-01 00:00,70
        2023-10-01 01:00,71
        2023-10-01 02:00,72
        2023-10-01 03:00,73
        2023-10-01 04:00,72
        2023-10-01 05:00,71
        2023-10-01 06:00,70
        """

        m.get(urls[0], content=bytes(csv_data, 'utf-8'))
        max_temp = get_max_temp('http://example.com/', 'data.csv')
        self.assertEqual(max_temp, 73)


if __name__ == '__main__':
    unittest.main()
