from pyspark import rdd
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, unix_timestamp, concat, lit
import zipfile
import os
import tempfile


def main():
    avg_trip_duration_per_day(create_data_frame())


def create_data_frame():
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()
    directory_path = "./data"
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


def avg_trip_duration_per_day(dataframes):
    combined_df = None
    output_path = "reports/analysis_1"
    for df in dataframes:
        if "start_time" in df.columns:
            df = df.withColumn("day", to_date(col("start_time")))
            df = df.withColumn("trip_duration", (unix_timestamp("end_time") - unix_timestamp("start_time")))
        elif "started_at" in df.columns:
            df = df.withColumn("day", to_date(col("started_at")))
            df = df.withColumn("trip_duration", (unix_timestamp("ended_at") - unix_timestamp("started_at")))
        common_columns = ["day", "trip_duration"]
        df = df.select(common_columns)

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    not_sorted_result = combined_df.groupBy("day").agg({"trip_duration": "avg"})
    result = not_sorted_result.orderBy("day")
    result.coalesce(1).write.option("header", "true").csv(output_path)

    files = os.listdir(output_path)
    for file in files:
        if file.endswith(".csv"):
            csv_file_path = os.path.join(output_path, file)
            new_csv_file_path = os.path.join(output_path, "average_trip_duration_per_day.csv")
            os.rename(csv_file_path, new_csv_file_path)
        else:
            file_to_delete = os.path.join(output_path, file)
            os.remove(file_to_delete)


if __name__ == "__main__":
    main()
