from pyspark.sql import SparkSession
import pandas as pd
import zipfile


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    df = spark.read.csv("./Divvy_Trips_2019_Q4.csv", header=True, inferSchema=True)

    # Show the DataFrame
    df.show()

    # Stop the Spark session when you're done
    spark.stop()


if __name__ == "__main__":
    main()
