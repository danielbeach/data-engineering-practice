from main import add_new_columns
import zipfile
import csv
import io
import pytest
from pyspark.sql import Row
import pyspark.sql.functions as F

def test_add_new_columns(tmp_path,spark):

    #Arrange
    data = [
        ('2022-01-01','ZLW18P9K','ST14000NM001G',14000519643136),
        ('2022-01-01','AZZLW18P9K','AZ ST14000NM001G',14000519643136)
    ]

    columns=['date','serial_number','model','capacity_bytes']

    df = spark.createDataFrame(data,columns)

    output_folder=tmp_path / 'test_hard-drive-2022-01-01-failures.csv'

    #Act
    result = add_new_columns([str(output_folder)],df)


    #Assert
    assert result.schema["file_date"].dataType.simpleString() == 'date'
    expected_data = [
        Row(date="2022-01-01",serial_number='ZLW18P9K', model='ST14000NM001G',capacity_bytes=14000519643136,source_file='test_hard-drive-2022-01-01-failures',file_date='2022-01-01',brand='unkown'),
        Row(date="2022-01-01",serial_number='AZZLW18P9K', model='AZ ST14000NM001G',capacity_bytes=14000519643136,source_file='test_hard-drive-2022-01-01-failures',file_date='2022-01-01',brand='AZ'),
        
    ]
    expected_df=spark.createDataFrame(expected_data).withColumn('file_date',F.col('file_date').cast('date'))
    actual = result.collect()
    expected = expected_df.collect()

    assert actual == expected