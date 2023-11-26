import tempfile
import duckdb
import pytest
import os
from main import db_work


def test_export_list():
    temp_test_reports = tempfile.mkdtemp()
    db_work(temp_test_reports) 
    all_items = sorted(os.listdir(temp_test_reports))
    result = ['car_by_model_year.parquet', 'electric_car_per_city.parquet', 'top_cars.parquet', 'vehicle_by_postcode.parquet']

    assert all_items == result

if __name__ == '__main__':
    pytest.main()
