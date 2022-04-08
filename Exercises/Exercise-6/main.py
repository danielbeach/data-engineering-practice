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


def main():
    # extract the zip-files
    extract_files()

    # create directory to save reports
    create_reports()

    # create spark session
    spark = SparkSession.builder.appName('Exercise6').master("local[*]").enableHiveSupport().getOrCreate()

    trips_2019 = spark.read.option("header", "true").option("inferSchema", "true").csv("data/Divvy_Trips_2019_Q4.csv")
    trips_2020 = spark.read.option("header", "true").option("inferSchema", "true").csv("data/Divvy_Trips_2020_Q1.csv")

    # trips_2019.printSchema()

    # calculate the average trip duration per day
    # avg_tripDuration(trips_2019)

    # calculate the number of trip duration per day
    # numTrips_perDay(trips_2019)


if __name__ == '__main__':
    main()

#  spark-submit main.py
