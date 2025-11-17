from main import add_primary_key
import zipfile
import csv
import io
import pytest
from pyspark.sql import Row
import pyspark.sql.functions as F

@pytest.mark.parametrize(
      'num_hashed_cols,dataset',
      [
          (1,
           [
            ('2022-01-01','ZLW18P9K','ST14000NM001G',14000519643136),
            ('2022-01-01','AZZLW18P9K','AZ ST14000NM001G',14000519643136)
            ]
           ),
           (2,
           [
            ('2022-01-01','ZLW18P9K','ST14000NM001G',14000519643136),
            ('2022-01-01','ZLW18P9K','AZ ST14000NM001G',14000519643136)
            ]
           ),
           (3,
           [
            ('2022-01-01','ZLW18P9K','ST14000NM001G',14000519643136),
            ('2022-01-01','ZLW18P9K','ST14000NM001G',14000519643137)
            ]
           )

      ]  
)
def test_add_primary_key(spark,num_hashed_cols,dataset):
    #Arrange
    # data = [
    #     ('2022-01-01','ZLW18P9K','ST14000NM001G',14000519643136),
    #     ('2022-01-01','AZZLW18P9K','AZ ST14000NM001G',14000519643136)
    # ]

    columns=['date','serial_number','model','capacity_bytes']

    df = spark.createDataFrame(dataset,columns)

    #Act
    result = add_primary_key(df)


    #Assert
    if num_hashed_cols==1:
        expected_data = [
            Row(date="2022-01-01",serial_number='ZLW18P9K', model='ST14000NM001G',capacity_bytes=14000519643136,primary_key=-679218196),
            Row(date="2022-01-01",serial_number='AZZLW18P9K', model='AZ ST14000NM001G',capacity_bytes=14000519643136,primary_key=-734161738),
            
        ]
        expected_df=spark.createDataFrame(expected_data)
        actual = result.collect()
        expected = expected_df.collect()

        assert actual == expected
    if num_hashed_cols==2:
        expected_data = [
            Row(date="2022-01-01",serial_number='ZLW18P9K', model='ST14000NM001G',capacity_bytes=14000519643136,primary_key=1062911449),
            Row(date="2022-01-01",serial_number='ZLW18P9K', model='AZ ST14000NM001G',capacity_bytes=14000519643136,primary_key=-124218605),
            
        ]
        expected_df=spark.createDataFrame(expected_data)
        actual = result.collect()
        expected = expected_df.collect()

        assert actual == expected
    
    if num_hashed_cols==3:
        expected_data = [
            Row(date="2022-01-01",serial_number='ZLW18P9K', model='ST14000NM001G',capacity_bytes=14000519643136,primary_key=850339449),
            Row(date="2022-01-01",serial_number='ZLW18P9K', model='ST14000NM001G',capacity_bytes=14000519643137,primary_key=-1971518983),
            
        ]
        expected_df=spark.createDataFrame(expected_data)
        actual = result.collect()
        expected = expected_df.collect()

        assert actual == expected