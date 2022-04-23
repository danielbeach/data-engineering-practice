import os
from pyspark.sql import SparkSession
import zipfile
from pyspark.sql import functions as func
from concurrent.futures import ThreadPoolExecutor


# create reports directory if it does not exist
def create_reports():
    if os.path.isdir("reports"):
        pass
    else:
        os.mkdir("reports")


# extract all zip files
def extract_files():
    zip_files = os.listdir("data")

    def extract(file):
        zip_object = zipfile.ZipFile(f"data/{file}", "r")
        zip_object.extractall("data/")
        zip_object.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(extract, zip_files)


# calculate average trips per day
def avg_tripDuration(df):
    trips_date = df.select(func.col("tripduration").cast("numeric"), func.to_date(func.col("end_time")).alias("date"))
    avg_trip = trips_date.groupby(func.col("date")).avg("tripduration").alias("avg_duration").orderBy(func.col("date"))
    avgtrips_perday = avg_trip.select(func.col("date"), func.col("avg(tripduration)").alias("average_tripDuration"))

    # save file in reports directory
    avgtrips_perday.write.option("header", "true") \
        .csv(path="reports/avgTrips_perDay",
             mode="overwrite")


def numTrips_perDay(df):
    trips_select = df.select(func.col("tripduration").cast("numeric"), func.to_date(func.col("end_time")).alias("date"))
    trip_count = trips_select.groupby(func.col("date")).count().alias("avg_duration").orderBy(func.col("date"))
    trips_per_day = trip_count.select(func.col("date"), func.col("count").alias("num_of_trips"))

    # save file in reports directory
    trips_per_day.write.option("header", "true") \
        .csv(path="reports/trips_per_day",
             mode="overwrite")


def mostPopular_tripStation(df):
    pass


def main():
    # extract the zip-files
    extract_files()

    # create directory to save reports
    create_reports()

    # create spark session
    spark = SparkSession.builder.appName('Exercise6').master("local[*]").enableHiveSupport().getOrCreate()

    trips_2019 = spark.read.option("header", "true").option("inferSchema", "true").csv("data/Divvy_Trips_2019_Q4.csv")
    trips_2020 = spark.read.option("header", "true").option("inferSchema", "true").csv("data/Divvy_Trips_2020_Q1.csv")

    t = trips_2019.select(func.col("from_station_name"), func.month(func.col("end_time")).alias("month"))
    td = t.groupby(func.col("from_station_name"), func.col("month")).count().orderBy("month", "count", ascending=False)
    data_select = td.select(func.col("from_station_name"), func.col("month"), func.col("count").alias("num_of_trips"))
    ts = td.groupby(func.col("month")).max("count")

    max_trips = []
    for t in ts.select(func.col("max(count)").alias("num_of_trips")).rdd.collect():
        max_trips.append(t[0])
    print(max_trips)

    data_select.filter(func.col("count").isin(max_trips)).orderBy("month", "num_of_trips", ascending=False).limit(5).show()
    # data_select.filter(data_select.num_of_rips == list(ts))
    # trips_2019.printSchema()

    # calculate the average trip duration per day
    # avg_tripDuration(trips_2019)

    # calculate the number of trip duration per day
    # numTrips_perDay(trips_2019)


if __name__ == '__main__':
    main()

#  spark-submit main.py

import os
from pyspark.sql import SparkSession
import zipfile
from pyspark.sql import functions as func
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Window


# create reports directory if it does not exist
def create_reports():
    if os.path.isdir("reports"):
        pass
    else:
        os.mkdir("reports")


# extract all zip files
def extract_files():
    zip_files = os.listdir("data")

    def extract(file):
        zip_object = zipfile.ZipFile(f"data/{file}", "r")
        zip_object.extractall("data/")
        zip_object.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(extract, zip_files)


# calculate average trips per day
def avg_tripDuration(df):
    trips_date = df.select(func.col("tripduration").cast("numeric"), func.to_date(func.col("end_time")).alias("date"))
    avg_trip = trips_date.groupby(func.col("date")).avg("tripduration").alias("avg_duration").orderBy(func.col("date"))
    avgtrips_perday = avg_trip.select(func.col("date"), func.col("avg(tripduration)").alias("average_tripDuration"))

    # save file in reports directory
    avgtrips_perday.write.option("header", "true") \
        .csv(path="reports/avgTrips_perDay",
             mode="overwrite")


def numTrips_perDay(df):
    trips_select = df.select(func.col("tripduration").cast("numeric"), func.to_date(func.col("end_time")).alias("date"))

    trip_count = trips_select.groupby(func.col("date")).count().alias("avg_duration").orderBy(func.col("date"))

    trips_per_day = trip_count.select(func.col("date"), func.col("count").alias("num_of_trips"))

    # save file in reports directory
    trips_per_day.write.option("header", "true") \
        .csv(path="reports/trips_per_day",
             mode="overwrite")


def mostPopular_tripStation(df):
    # select the necessary columns needed
    station = df.select(func.col("from_station_name"), func.month(func.col("end_time")).alias("month"))

    # group column by station name
    station_trip = station.groupby(func.col("from_station_name"), func.col("month")).count() \
        .orderBy("month", "count", ascending=False)

    # rename columns and add row number to each row
    most_station_trip = station_trip.select(func.col("from_station_name"), func.col("month"), func.col("count") \
                                            .alias("num_of_trips")) \
        .withColumn("row_number",
                    func.row_number().over(Window.partitionBy("month").orderBy(func.desc("num_of_trips")))) \
        .filter(func.col("row_number") == 1)

    # save file in reports directory
    most_station_trip.write.option("header", "true") \
        .csv(path="reports/most_popular_station",
             mode="overwrite")


def top_daily_station(df):
    trip_start = df.select(func.col("from_station_name"), func.to_date(func.col("end_time")).alias("date"))
    trip_end = df.select(func.col("to_station_name"), func.to_date(func.col("end_time")).alias("date"))

    # count station and end station
    station_count_start = trip_start.groupby(func.col("from_station_name"), func.col("date")).count()
    station_count_end = trip_end.groupby(func.col("to_station_name"), func.col("date")).count()

    # select the necessary columns
    start_select = station_count_start.select(func.col("from_station_name"), func.col("date"),
                                              func.col("count").alias("num_of_trips1"))
    end_select = station_count_end.select(func.col("to_station_name"), func.col("date").alias("date2"),
                                          func.col("count").alias("num_of_trips2"))

    # noinspection PyTypeChecker
    # join both columns on date and station name
    start_end = start_select.join(end_select, [start_select.from_station_name == end_select.to_station_name,
                                               start_select.date == end_select.date2])

    # calculate the total trips for stations
    trip_count = start_end.withColumn("num_of_trips", func.col("num_of_trips1") + func.col("num_of_trips2")) \
        .select(func.col("from_station_name").alias("station_name"), func.col("date").alias("date"),
                func.col("num_of_trips")) \
        .orderBy("date", func.desc("num_of_trips"))

    # add row number to each rows
    completed_trip = trip_count.withColumn("row_number", func.row_number().over(
        Window.partitionBy("date").orderBy(func.desc("num_of_trips")))) \
        .filter(func.col("row_number") <= 3).select("station_name", "date", "num_of_trips").limit(42)

    # save file in reports directory
    completed_trip.write.option("header", "true") \
        .csv(path="reports/top_daily_station",
             mode="overwrite")


def main():
    # extract the zip-files
    extract_files()

    # create directory to save reports
    create_reports()

    # create spark session
    spark = SparkSession.builder.appName('Exercise6').master("local[*]").enableHiveSupport().getOrCreate()

    trips_2019 = spark.read.option("header", "true").option("inferSchema", "true").csv("data/Divvy_Trips_2019_Q4.csv")
    trips_2020 = spark.read.option("header", "true").option("inferSchema", "true").csv("data/Divvy_Trips_2020_Q1.csv")

    # # calculate the average trip duration per day
    # avg_tripDuration(trips_2019)
    #
    # # calculate the number of trip duration per day
    # numTrips_perDay(trips_2019)
    #
    # # calculate most popular station
    # mostPopular_tripStation(trips_2019)

    # top three stations daily
    # top_daily_station(trips_2019)


if __name__ == '__main__':
    main()

#  spark-submit main.py

