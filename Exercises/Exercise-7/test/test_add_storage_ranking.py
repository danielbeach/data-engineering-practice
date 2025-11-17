from main import add_storage_ranking
import zipfile
import csv
import io
import pytest
from pyspark.sql import Row
import pyspark.sql.functions as F

def test_add_storage_ranking(spark):
    #Arrange
    data = [#[-1.0, 500107862016.0, 6001175126016.0, 12000138625024.0, 18000207937536.0]
        ('2022-01-01','ZLW18P9K','ST14000NM001G',12000138625024),
        ('2022-01-01','AZZLW18P9K','AZ ST14000NM001G',6001175126016),
        ('2022-01-01','AZZLW18P9K','AZ ST14000NM001G',6001175126016),
        ('2022-01-01','AZZLW18P9K','AG ST14000NM001G',500107862016),
        ('2022-01-01','AZZLW18P9K','AN ST14000NM001G',519643136)
    ]

    columns=['date','serial_number','model','capacity_bytes']

    df = spark.createDataFrame(data,columns)

    #Act
    result = add_storage_ranking(df)


    #Assert
    expected_data = [
        Row(date="2022-01-01",serial_number='ZLW18P9K', model='ST14000NM001G',capacity_bytes=12000138625024,capacity_ranking='Huge'),
        Row(date="2022-01-01",serial_number='AZZLW18P9K', model='AZ ST14000NM001G',capacity_bytes=6001175126016,capacity_ranking='Huge'),
        Row(date="2022-01-01",serial_number='AZZLW18P9K', model='AZ ST14000NM001G',capacity_bytes=6001175126016,capacity_ranking='Huge'),
        Row(date="2022-01-01",serial_number='AZZLW18P9K', model='AG ST14000NM001G',capacity_bytes=500107862016,capacity_ranking='Large'),
        Row(date="2022-01-01",serial_number='AZZLW18P9K', model='AN ST14000NM001G',capacity_bytes=519643136,capacity_ranking='Medium')
        
    ]
    expected_df=spark.createDataFrame(expected_data)
    #actual = result.collect()
    #expected = expected_df.collect()
    actual = sorted(result.collect(), key=lambda r: r['capacity_bytes'],reverse=True)
    expected = sorted(expected_df.collect(), key=lambda r: r['capacity_bytes'],reverse= True)

    assert actual == expected