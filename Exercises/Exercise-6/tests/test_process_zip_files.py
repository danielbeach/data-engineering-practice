from main import process_zip_files
import zipfile
import csv
import io
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Fixture to create a local SparkSession for testing."""
    return SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

@pytest.fixture
def dummy_zip_file(tmp_path):
    """Creates a dummy zip file containing a CSV and returns its path."""

    zip_paths = [tmp_path / "dummy.zip",tmp_path / "dummy2.zip"]

    #preapare dummy csv content

    for path in zip_paths:
        csv_content = io.StringIO()
        writer = csv.writer(csv_content)
        writer.writerow(["id", "name", "age"])
        writer.writerow([1, "Alice", 30])
        writer.writerow([2, "Bob", 25])

        #write to zip
        with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
            z.writestr("test.csv",csv_content.getvalue())

    return zip_paths

@pytest.mark.parametrize(
    "expected_data",
    [
        [['id,name,age', '1,Alice,30', '2,Bob,25'],
        ['id,name,age', '1,Alice,30', '2,Bob,25']]
    ]
)
def test_process_zip_files(dummy_zip_file,expected_data,spark):
    #act
    
    result = process_zip_files(spark,dummy_zip_file)#spark.sparkContext.parallelize(dummy_zip_file).flatMap(process_zip_files)#process_zip_files(dummy_zip_file)

    print(result[0].collect())
    #assert
    for i in range(0,len(result)-1):
        assert result[i].collect() == expected_data[i]
    