from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, avg, count, date_trunc, date_format, rank, max, date_add,lit, round, current_date, year, when 
from pyspark.sql.window import Window
import zipfile
from io import StringIO,BytesIO
import os
from pathlib import Path 

ZIP_FOLDER='./data'
#Define global variable: reports folder path
REPORTS_FOLDER_PATH= Path.cwd()/"reports"
#Create folder if not exists
REPORTS_FOLDER_PATH.mkdir(parents=True,exist_ok=True)

def process_zip_files(spark,zip_file_paths:list):
    """
    This function will be applied to each zip file path. 
    It opens the zip file, iterates through its contents, and extracts CSV files into a list of strings.
    """
    all_rdds=[]
    for zip_path in zip_file_paths:
        #open zip file as binary
        with open(zip_path, 'rb') as f:
            
            with zipfile.ZipFile(BytesIO(f.read())) as z:
                for name in z.namelist():
                    if name.endswith('.csv'):
                        #readd csv file a text
                        with z.open(name) as csv_file:
                            content = csv_file.read().decode("utf-8",errors="ignore").splitlines()
                            rdd = spark.sparkContext.parallelize(content)
                            all_rdds.append(rdd)
    return all_rdds


def avg_trip_duration_per_day(folder,df):
    """
    Calculates the average trip duration per day.
    Saves the result in a csv report
    """
    print("=============================Creating Report: avg_trip_duration_per_day.csv=============================")
    df = df.withColumn('trip_date', col('start_time').cast('date'))
    df = df.select(['trip_date','tripduration'])
    query = df.groupBy('trip_date').agg(round(avg('tripduration'),2).alias('avg_trip_duration'))\
            .orderBy(col('trip_date').asc())
    query = query.filter(col('trip_date').isNotNull())
    query.coalesce(1).write.csv(f'{folder}/avg_trip_duration.csv',header=True,mode='overwrite')
    print("=============================Report Available: avg_trip_duration_per_day.csv=============================")
    return True

def qty_trips_per_day(folder,df):
    """
    Calculates the number of trips per day.
    Saves the result in a csv report
    """
    print("=============================Creating Report: qty_trips_per_day.csv=============================")
    df = df.withColumn('trip_date', col('start_time').cast('date'))
    df = df.select(['trip_date','trip_id'])
    query = df.groupBy('trip_date').agg(count('trip_id').alias('qty_of_trips')).orderBy(col('trip_date').asc())
    query = query.filter(col('trip_date').isNotNull())
    query.coalesce(1).write.csv(f'{folder}/qty_trips_per_day.csv',header=True,mode='overwrite')
    print("=============================Report Available: qty_trips_per_day.csv=============================")
    
    return True

def most_popular_starting_station_month(folder,df):
    """
    Retrive the most popular starting trip stations per month.
    Saves the result in a csv report
    """
    print("=============================Creating Report: most_popular_starting_station_month.csv=============================")
    df = df.withColumn('trip_date', col('start_time').cast('date'))
    df = df.withColumn('month_trip_date',date_trunc('month',df['trip_date']))
    df = df.withColumn('year_month',date_format(df['month_trip_date'],'yyyy-MM'))
    df = df.select(['year_month','from_station_id','from_station_name','trip_id'])
    query = df.groupBy(['year_month','from_station_id','from_station_name']).agg(count('trip_id').alias('qty_of_trips'))
    window = Window.partitionBy("year_month").orderBy(col("qty_of_trips").desc())
    query = query.withColumn("rank",rank().over(window)).filter(col("rank")==1).drop("rank")
    query = query.filter(col('year_month').isNotNull())
    query.coalesce(1).write.csv(f'{folder}/most_popular_starting_station_month.csv',header=True,mode='overwrite')
    print("=============================Report Available: most_popular_starting_station_month.csv=============================")

    return True

def top_3_stations_day(folder,df):##FINISH
    """
    Retrive the 3 most popular starting trip stations per day in the last 2 weeks.
    Saves the result in a csv report
    """
    print("=============================Creating Report: top_3_stations_day.csv=============================")
    df = df.withColumn('trip_date', col('start_time').cast('date'))
    max_date= df.agg(max("trip_date").alias("max_date")).collect()[0][0]
    last_two_weeks = date_add(lit(max_date),-14)
    df = df.select(['trip_date','from_station_id','from_station_name','trip_id']).filter(col("trip_date")>=last_two_weeks)
    query = df.groupBy(['trip_date','from_station_id','from_station_name']).agg(count('trip_id').alias('qty_of_trips'))
    window = Window.partitionBy("trip_date").orderBy(col("qty_of_trips").desc())
    query = query.withColumn("rank",rank().over(window)).filter(col("rank")<=3).drop("rank")
    query = query.orderBy(col("trip_date").asc())
    query.coalesce(1).write.csv(f'{folder}/top_3_stations_day.csv',header=True,mode='overwrite')
    print("=============================Report Available: top_3_stations_day.csv=============================")

    return True


def avg_trip_duration_per_gender(folder,df):
    """
    Calculates the average trip duration per gender.
    Saves the result in a csv report
    """
    print("=============================Creating Report: avg_trip_duration_per_gender.csv=============================")
    df = df.filter((col("gender") == "Male") | (col("gender") == "Female"))
    df = df.select(['gender','tripduration'])
    query = df.groupBy('gender').agg(round(avg('tripduration'),2).alias('avg_trip_duration'))
    query.coalesce(1).write.csv(f'{folder}/avg_trip_duration_gender.csv',header=True,mode='overwrite')
    print("=============================Report Available: avg_trip_duration_per_gender.csv=============================")


    return True

def top_ten_ages(folder,df,duration='longest'):
    """
    Calculates the top 10 ages with the longest/shortest duration.
    Saves the result in a csv report.
    The parameter duration take the following str values: longest, shortest.
    """
    df = df.filter(col("birthyear").isNotNull())
    df = df.select(col('birthyear'),col('tripduration'),year(current_date()).alias("current_year"))
    df =  df.select((col('current_year')-col('birthyear')).alias("age"),'tripduration')
    if duration == 'longest':
        print("=============================Creating Report: top_ten_ages_longest.csv=============================")
        query = df.groupBy('age').agg(round(avg('tripduration'),2).alias('avg_trip_duration')).orderBy(col('avg_trip_duration').desc()).limit(10)
        query.coalesce(1).write.csv(f'{folder}/top_ten_ages_longest.csv',header=True,mode='overwrite')
        print("=============================Report Available: top_ten_ages_longest.csv=============================")
    else:
        print("=============================Creating Report: top_ten_ages_shortest.csv=============================")
        query = df.groupBy('age').agg(round(avg('tripduration'),2).alias('avg_trip_duration')).orderBy(col('avg_trip_duration').asc()).limit(10)
        query.coalesce(1).write.csv(f'{folder}/top_ten_ages_shortest.csv',header=True,mode='overwrite')
        print("=============================Report Available: top_ten_ages_shortest.csv=============================")


    return True

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    
    zip_file_paths=[]
    for file in os.listdir(ZIP_FOLDER):
        if file.endswith('.zip'):
            zip_file_path = os.path.join(ZIP_FOLDER,file)
            zip_file_paths.append(zip_file_path)

    #create RDD of each zip file content
    rdd_of_csv_contents=process_zip_files(spark,zip_file_paths)

    #merge all RDDs into one
    combined_rdd=spark.sparkContext.union(rdd_of_csv_contents)

    # convert RDD of lines into spark Dataframe 
    custom_schema= StructType([
        StructField('trip_id',StringType(),True),
        StructField('start_time',TimestampType(),True),
        StructField('end_time',TimestampType(),True),
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
    df = spark.read.csv(combined_rdd, header=True, schema=custom_schema)
    
    #show metadata
    df.printSchema()
    print(df.dtypes)
    df = df.withColumn(
        "tripduration", when(col("tripduration").isNull(), 
                             (col("end_time").cast("long") - col("start_time").cast("long")).cast("double") ).\
                        otherwise(col("tripduration"))
    )
    df.show()
    

    avg_trip_duration_per_day(REPORTS_FOLDER_PATH,df)
    qty_trips_per_day(REPORTS_FOLDER_PATH,df)
    most_popular_starting_station_month(REPORTS_FOLDER_PATH,df)
    top_3_stations_day(REPORTS_FOLDER_PATH,df)
    avg_trip_duration_per_gender(REPORTS_FOLDER_PATH,df)
    top_ten_ages(REPORTS_FOLDER_PATH,df)
    top_ten_ages(REPORTS_FOLDER_PATH,df,'shortest')



if __name__ == "__main__":
    main()
