from pyspark.sql import SparkSession
import os
import zipfile
import tempfile
from pyspark.sql.functions import lit, regexp_extract, to_date, when, split, sha1, concat_ws, dense_rank
from pyspark.sql.window import Window


def main():
    create_spark("./data", "./result", "exercise7.csv")


def create_spark(directory_path, output_path, name):
    spark = SparkSession.builder.appName("Exercise-7").getOrCreate()
    df = create_df(directory_path, spark)
    df = extract_date_from_source_file(df)
    output_result(df, output_path, name)
    spark.stop()


def create_df(directory_path, spark):
    df = None

    for filename in os.listdir(directory_path):
        if filename.endswith(".zip"):
            zip_file_path = os.path.join(directory_path, filename)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if '__MACOSX' not in file_info.filename and file_info.filename.endswith(".csv"):
                        with tempfile.NamedTemporaryFile(delete=False) as temp_csv_file:
                            with zip_file.open(file_info) as csv_file:
                                temp_csv_file.write(csv_file.read())

                        current_df = spark.read.csv(temp_csv_file.name, header=True, inferSchema=True)
                        current_df = current_df.withColumn("source_file", lit(filename))

                        if df is None:
                            df = current_df
                        else:
                            df = df.union(current_df)

    return df


def extract_date_from_source_file(df):
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    df = df.withColumn("file_date", to_date(regexp_extract(df["source_file"], date_pattern, 1)))
    df = df.withColumn("brand", when(df["model"].contains(" "), split(df["model"], " ")[0]).otherwise("unknown"))

    new_df = df.select("capacity_bytes")
    new_df = new_df.orderBy("capacity_bytes")
    window_spec = Window.orderBy(df["capacity_bytes"].desc())
    new_df = new_df.select("capacity_bytes", dense_rank().over(window_spec).alias("rank"))
    new_df = new_df.dropDuplicates(["rank"])

    new_df = new_df.withColumnRenamed("capacity_bytes", "new_capacity_bytes")

    result_df = df.join(new_df, df["capacity_bytes"] == new_df["new_capacity_bytes"], "inner")
    result_df = result_df.drop("new_capacity_bytes")

    unique_columns = ["serial_number", "failure", "model"]
    df = result_df.withColumn("primary_key", sha1(concat_ws("_", *[df[column] for column in unique_columns])))

    return df


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
            print(output_path)
            print(f"!!!File {name} already exists!!!")
        else:
            print(e)


if __name__ == "__main__":
    main()
