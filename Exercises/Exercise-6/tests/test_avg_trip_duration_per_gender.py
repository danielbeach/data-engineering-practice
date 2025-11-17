from main import avg_trip_duration_per_gender
import zipfile
import csv
import io
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import Row



def test_avg_trip_duration_per_gender(tmp_path,spark):
    #Arrange
    #create a sample DataFrame
    data = [
        ('25223640', "2019-10-01 00:01:39", '2019-10-01 00:17:20', 2215, 940.0,20, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ('25223641', '2019-10-01 00:01:39', '2019-10-01 00:17:20', 2215, 255.0,20, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ('25223641', '2019-10-16 00:01:39', '2019-10-16 00:17:20', 2215, 200.0,21, 'Sheffield', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 940.0,20, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 200.0,20, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 255.0,20, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Female', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 940.0,20, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Female', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 200.0,21, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Female', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 940.0,21, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Female', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 200.0,21, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Female', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 255.0,22, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Female', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 200.0,22, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ('25223640', "2019-10-18 00:01:39", '2019-10-18 00:17:20', 2215, 255.0,23, 'Sheffield Ave & K', 309, 'Leavitt St & Armi', 'Subscriber', 'Male', 1987), 
        ]
    # columns= ['trip_id',
    #           'start_time',
    #           'end_time',
    #           'bikeid',
    #           'tripduration',
    #           'from_station_id',
    #           'from_station_name',
    #           'to_station_id',
    #           'to_station_name'
    #           'usertype',
    #           'gender',
    #           'birthyear']
    custom_schema= StructType([
        StructField('trip_id',StringType(),True),
        StructField('start_time',StringType(),True),
        StructField('end_time',StringType(),True),
        StructField('bikeid',IntegerType(),True),
        StructField('tripduration',DoubleType(),True),
        StructField('from_station_id',IntegerType(),True),
        StructField('from_station_name',StringType(),True),
        StructField('to_station_id',IntegerType(),True),
        StructField('to_station_name',StringType(),True),
        StructField('usertype',StringType(),True),
        StructField('gender',StringType(),True),
        StructField('birthyear',IntegerType(),True)
    ])
    df=spark.createDataFrame(data,custom_schema)

    output_folder=tmp_path

    #Act
    result = avg_trip_duration_per_gender(str(output_folder),df)

    #Assert
    #assert the function return True
    assert result is True

    #assert the csv exists
    output_dir = output_folder / 'avg_trip_duration_gender.csv'
    assert output_dir.exists()

    #validate the logic
    written_df = spark.read.csv(str(output_dir),header=True,inferSchema=True)
    expected_data = [
        Row(gender="Female",avg_trip_duration=465.0),
        Row(gender="Male",avg_trip_duration=427.14)

    ]
    expected_df=spark.createDataFrame(expected_data)
    actual = sorted(written_df.collect(), key=lambda r: r['avg_trip_duration'])
    expected = sorted(expected_df.collect(), key=lambda r: r['avg_trip_duration'])

    assert actual == expected

