from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    # your code here


if __name__ == "__main__":
    main()
