from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
# from great_expectations.core.batch import RuntimeBatchRequest
# #from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler
# from great_expectations.data_context import BaseDataContext
# from great_expectations.data_context.types.base import (
#     DataContextConfig,
#     FilesystemStoreBackendDefaults,
# )
import great_expectations as gx



def main():
    


    # Create a SparkSession
    spark = SparkSession.builder.appName("BikeRideDuration").getOrCreate()

    # Define the schema based on the provided CSV structure
    schema = StructType([
        StructField("ride_id", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("ended_at", StringType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lng", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lng", DoubleType(), True),
        StructField("member_casual", StringType(), True),
    ])

    input_csv_path = "data/202306-divvy-tripdata.csv"

    df = spark.read.csv(
        input_csv_path,
        header=True,
        schema=schema,
        mode="DROPMALFORMED"
    )

    df = df.withColumn(
        "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
    )

    df = df.withColumn(
        "duration_seconds",
        unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
    )

    df = df.withColumn(
        "date", date_format(col("started_at"), "yyyy-MM-dd")
    )

    daily_durations = df.groupBy("date").agg(
        _sum("duration_seconds").alias("total_duration_seconds")
    )
    #daily_durations.filter(col("total_duration_seconds")>86400).show()
    
    #Create GX data context
    context = gx.get_context()

    #Connect to data and create a Batch
    data_source_name = "DataFrame_Trips_Source"
    data_source = context.data_sources.add_spark(name=data_source_name)
    data_asset_name= "DataFrame_Trips_DataAssest"
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)
    batch_definition_name = "trips_batch_definition"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
                                    batch_definition_name
                        )  
    batch_parameters = {"dataframe": daily_durations} 
    
    # Create an Expectation to test
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="total_duration_seconds",
        min_value=0,
        max_value=86400
    )

    # Get the dataframe as a Batch
    # batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # # Test the Expectation
    # validation_results = batch.validate(expectation)
    # print(validation_results)
    
    #Create Expectations suits
    expectation_suite_name = "trips_expectation_suite"
    expectation_suite = gx.ExpectationSuite(name=expectation_suite_name)

    #Add Expectation Suit to Datacontext
    expectation_suite = context.suites.add(expectation_suite)

    #Add Expectations to Suit
    expectation_suite.add_expectation(expectation)
    
    #Create Validation Definition
    definition_name = 'trips_validation_definition'
    validation_definition = gx.ValidationDefinition(
                                    data=batch_definition, suite=expectation_suite, name=definition_name
                            )
    validation_definition = context.validation_definitions.add(validation_definition)
    validation_results = validation_definition.run(batch_parameters=batch_parameters)
    print(validation_results)

    output_parquet_path = "results/output_file.parquet"
    daily_durations.write.mode("overwrite").parquet(output_parquet_path)

if __name__ == "__main__":
    main()