from datetime import datetime, timedelta
from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_date, col, unix_timestamp, month, rank, count, row_number, avg
import zipfile
import os
import tempfile
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def main():
    combination_of_question("./data", "reports/analysis_1", "reports/analysis_2", "reports/analysis_3",
                            "reports/analysis_4", "reports/analysis_5", "reports/analysis_6")


def combination_of_question(directory_path, path_01, path_02, path_03, path_04, path_05, path_06):
    df = create_data_frame(directory_path)
    avg_trip_duration_per_day(df, path_01)
    trips_per_day(df, path_02)
    popular_month_station(df, path_03)
    top_station_by_day(df, path_04)
    longest_trips(df, path_05)
    top_ages(df, path_06)


def create_data_frame(directory_path):
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()
    dataframes = []

    for filename in os.listdir(directory_path):
        if filename.endswith(".zip"):
            zip_file_path = os.path.join(directory_path, filename)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if '__MACOSX' not in file_info.filename and file_info.filename.endswith(".csv"):
                        with tempfile.NamedTemporaryFile(delete=False) as temp_csv_file:
                            with zip_file.open(file_info) as csv_file:
                                temp_csv_file.write(csv_file.read())

                        df = spark.read.csv(temp_csv_file.name, header=True, inferSchema=True)
                        dataframes.append(df)
    return dataframes


def avg_trip_duration_per_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        if "start_time" in df.columns:
            df = df.withColumnRenamed("start_time", "started_at")
            df = df.withColumnRenamed("from_station_name", "start_station_name")
            df = df.withColumnRenamed("end_time", "ended_at")
        df = df.withColumn("day", to_date(col("started_at")))
        df = df.withColumn("trip_duration", (unix_timestamp("ended_at") - unix_timestamp("started_at")))
        df = df.select("day", "trip_duration")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    not_sorted_result = combined_df.groupBy("day").agg({"trip_duration": "avg"})
    result = not_sorted_result.orderBy("day")

    output_result(result, output_path, "trip_duration.csv")


def trips_per_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        if "started_at" in df.columns:
            df = df.withColumnRenamed("started_at", "start_time")
        df = df.withColumn("date", to_date(col("start_time")))
        df = df.select("date")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    not_sorted_result = combined_df.groupBy("date").count().withColumnRenamed("count", "trip_count")
    result = not_sorted_result.orderBy("date")

    output_result(result, output_path, "trips_per_day.csv")


def popular_month_station(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        if "started_at" in df.columns:
            df = df.withColumnRenamed("started_at", "start_time")
            df = df.withColumnRenamed("start_station_name", "from_station_name")
        df = df.withColumn("month", month(col("start_time")))
        df = df.groupBy("month", "from_station_name").agg({"*": "count"})
        df = df.withColumnRenamed("count(1)", "trip_count")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    window_spec = Window.partitionBy("month").orderBy(col("trip_count").desc())
    popular_station_by_month = combined_df.withColumn("rank", rank().over(window_spec))
    popular_station_by_month = popular_station_by_month.filter(col("rank") == 1)
    popular_station_by_month = popular_station_by_month.select("month", "from_station_name", "trip_count")
    result = popular_station_by_month.orderBy("month")

    output_result(result, output_path, "popular_station_per_month.csv")


def top_station_by_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        if "start_time" in df.columns:
            df = df.withColumnRenamed("start_time", "started_at")
            df = df.withColumnRenamed("from_station_name", "start_station_name")

        df = filter_last_two_weeks(df, "started_at")
        df = df.select("started_at", "start_station_name")
        df = df.withColumn("started_at", to_date(df["started_at"]))
        df = df.groupBy("started_at", "start_station_name").agg(count("*").alias("trip_count"))
        windowSpec = Window.partitionBy("started_at").orderBy(F.desc("trip_count"))
        ranked_df = df.withColumn("station_rank", row_number().over(windowSpec))
        df = ranked_df.filter(F.col("station_rank") <= 3)

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    result = combined_df.orderBy("started_at")

    output_result(result, output_path, "top_station_by_day.csv")


def longest_trips(dataframes, output_path):
    for df in dataframes:
        try:
            df = df.select("start_time", "end_time", "gender")
            df = df.withColumn("trip_duration", (unix_timestamp("end_time") - unix_timestamp("start_time")))
            average_trip_duration = df.groupBy("gender").agg(avg("trip_duration"))
        except Exception as e:
            print(e)
            continue

        output_result(average_trip_duration, output_path, "longest_trips.csv")


def top_ages(dataframes, output_path):
    schema = StructType([
        StructField("birthyear", StringType(), True),
        StructField("trip_duration", DoubleType(), True)
    ])
    result_df = spark.createDataFrame([], schema)
    for df in dataframes:
        try:
            df = df.select("start_time", "end_time", "birthyear")
            df = df.withColumn("trip_duration", (unix_timestamp("end_time") - unix_timestamp("start_time")))
            df = df.select("birthyear", "trip_duration")
            df = df.filter(col("trip_duration") >= 0)
            df = df.filter(col("birthyear").cast("double").isNotNull())
            result_df = result_df.union(df)
        except Exception as e:
            print(e)
            continue

        result_df = result_df.orderBy("trip_duration", ascending=False)
        top_10 = result_df.limit(10)
        result_df = result_df.orderBy("trip_duration")
        bottom_10 = result_df.limit(10)
        final_result = top_10.union(bottom_10)

        output_result(final_result, output_path, "top_ages.csv")


def output_result(result, output_path, name):
    try:
        result.coalesce(1).write.option("header", "true").csv(output_path)

        files = os.listdir(output_path)
        for file in files:
            if file.endswith(".csv"):
                csv_file_path = os.path.join(output_path, file)
                new_csv_file_path = os.path.join(output_path, name)
                os.rename(csv_file_path, new_csv_file_path)
            else:
                file_to_delete = os.path.join(output_path, file)
                os.remove(file_to_delete)

    except Exception as e:
        if "exists" in str(e):
            print(f"!!!File {name} already exists!!!")
        else:
            print(e)


def filter_last_two_weeks(df, start):
    try:
        max_date = df.selectExpr(f"max({start})").collect()[0][0]
        max_datetime = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
        end_date = max_datetime
        start_date = max_datetime - timedelta(days=14)
        filtered_data = df.filter((col(start) >= start_date) & (col(start) <= end_date))
        return filtered_data
    except Exception as e:
        print(e)
        return df


if __name__ == "__main__":
    main()
