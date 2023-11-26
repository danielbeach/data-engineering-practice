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
