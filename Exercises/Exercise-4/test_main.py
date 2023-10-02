import os
import tempfile
import json
import pytest
from main import load_and_flatten_json_files


def test_load_and_flatten_json_files_with_non_existing_directory():
    non_existing_directory = '/non_existing_directory'

    with pytest.raises(FileNotFoundError) as e:
        load_and_flatten_json_files(non_existing_directory)
    assert 'Directory not found' in str(e.value)


def test_empty_directory():
    with tempfile.TemporaryDirectory() as temp_dir:
        flattened_data = load_and_flatten_json_files(temp_dir)
    assert flattened_data == []


def test_invalid_json():

    os.makedirs("invalid_json_test_directory", exist_ok=True)

    with open(os.path.join("invalid_json_test_directory", "invalid.json"), 'w') as file:
        file.write("invalwtgedsid JS/]/egz\ `ON 5yyesyxrgdfdata")

    with pytest.raises(json.JSONDecodeError) as e:
        load_and_flatten_json_files("invalid_json_test_directory")
    assert 'Expecting value' in str(e.value)

