import shutil
import tempfile
import zipfile
import io
import csv
import pandas as pd
import pytest
from unittest.mock import Mock, patch
from _pytest import pathlib
from main import combination_of_question
import os


# @pytest.fixture
# def csv_zip_files(tmp_path):
#     # Create a temporary directory for the zip files
#     temp_dir = tmp_path / "mocked_zip_folder"
#     temp_dir.mkdir()
#
#     zip_file1 = temp_dir / "file1.zip"
#
#     # Create an in-memory buffer to store the CSV data
#     csv_buffer = io.StringIO()
#     csv_writer = csv.writer(csv_buffer)
#     csv_writer.writerow(['header1', 'header2'])
#     csv_writer.writerow(['data1', 'data2'])
#
#     # Write the in-memory CSV data to the CSV file inside the ZIP archive
#     with zipfile.ZipFile(zip_file1, 'w') as zf:
#         with zf.open('file1.csv', 'w') as csv_file1:
#             csv_file1.write(csv_buffer.getvalue().encode('utf-8'))
#
#     csv_buffer.close()
#
#     return temp_dir
#
#
# def test_create_data_frame(csv_zip_files):
#
#     expected_df = "[DataFrame[header1: string, header2: string]]"
#
#     dataframes = create_data_frame(csv_zip_files)
#
#     assert str(dataframes) == expected_df


def test_combination_of_question():
    # Create a temporary directory for "test_reports"
    temp_test_reports = tempfile.mkdtemp()

    try:
        # Create a list of arguments with the temporary "test_reports" directory
        args = [
            "./huj",
            os.path.join(temp_test_reports, "analysis_1"),
            os.path.join(temp_test_reports, "analysis_2"),
            os.path.join(temp_test_reports, "analysis_3"),
            os.path.join(temp_test_reports, "analysis_4"),
            os.path.join(temp_test_reports, "analysis_5"),
            os.path.join(temp_test_reports, "analysis_6")
        ]

        combination_of_question(*args)

        expected_folders = ["analysis_1", "analysis_2", "analysis_3", "analysis_4", "analysis_5", "analysis_6"]

        all_items = os.listdir(temp_test_reports)

        folders = sorted([os.path.basename(item) for item in all_items if os.path.isdir(os.path.join(temp_test_reports, item))])
        expected_folders = sorted(expected_folders)

        assert folders == expected_folders
    finally:
        shutil.rmtree(temp_test_reports)


if __name__ == "__main__":
    test_combination_of_question()
