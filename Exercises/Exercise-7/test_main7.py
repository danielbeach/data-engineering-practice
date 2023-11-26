import os
import shutil
import tempfile
import time

from pyspark.sql import SparkSession

from main import create_spark


def test_create_spark():
    temp_test_reports = "./temp_test_reports"

    try:
        create_spark("./datatest", temp_test_reports, "exercise7.csv")
        spark = SparkSession.builder.appName("test-7").getOrCreate()

        path = os.path.join(temp_test_reports, "exercise7.csv")

        df = spark.read.csv(str(path), header=True, inferSchema=True)
        data_dict = df.rdd.map(lambda row: row.asDict()).collect()
        result = [{'date': '2022-01-01', 'serial_number': 'ZA1FLE1P', 'model': 'ST8000NM0055',
                   'capacity_bytes': 8001563222016, 'failure': 0, 'source_file': 'soft_were-2022-03-13-fail.zip',
                   'file_date': '2022-03-13', 'brand': 'unknown', 'rank': 2,
                   'primary_key': 'e0060b696e2b946c548009a4e600fdfb8816baf4'},
                  {'date': '2022-01-01', 'serial_number': '1050A084F97G', 'model': 'TOSHIBA MG07ACA14TA',
                   'capacity_bytes': 14000519643136, 'failure': 0, 'source_file': 'soft_were-2022-03-13-fail.zip',
                   'file_date': '2022-03-13', 'brand': 'TOSHIBA', 'rank': 1,
                   'primary_key': '2d79ebea27b0f4822f2c747b0e7f7d36ef2deb11'}]
        assert data_dict == result
        spark.stop()
    finally:
        shutil.rmtree(temp_test_reports)


if __name__ == "__main__":
    test_create_spark()
