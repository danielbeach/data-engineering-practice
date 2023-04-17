from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    # your code here


if __name__ == "__main__":
    main()
