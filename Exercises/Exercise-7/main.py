from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
import zipfile
from io import StringIO,BytesIO
import os

ZIP_FOLDER='./data'

def process_zip_files(spark,zip_file_paths:list):
    """
    This function will be applied to each zip file path. 
    It opens the zip file, iterates through its contents, and extracts CSV files into a list of strings.
    """
    print("============================= Unzipping File =============================")
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
    print("============================= File Unzipped =============================")
    return all_rdds

def add_new_columns(file_path,df):
    """
    Add a new columns to dataset
    """
    print("============================= ADDING NEW COLUMNS =============================")
    path = file_path[0]
    #get the name of the file
    file_name = (path.split('/')[-1]).split('.csv')[0]

    #pull the date from the file name
    date_pattern = r"(\d{4}-\d{2}-\d{2})"

    #add columns
    df=df.withColumn("source_file",F.lit(file_name))
    df= df.withColumn("file_date",F.regexp_extract(F.lit(file_name),date_pattern,1).cast("date"))

    #add brand
    df = df.withColumn('brand', F.when(F.col('model').contains(" "),
                                    F.split(F.col("model"), " ").getItem(0)).otherwise('unkown'))
    
    print("============================= NEW COLUMNS ADDED =============================")
    return df

def add_storage_ranking(df):
    """
    Add a column with rankings of the storage capacity based on the capacity bytes of the model.
    The ranking goes from 1 being the highest capacity to 4 being the lowest.
    """
    print("============================= ADDING STORAGE RANKING BUCKETS =============================")
    df_storage_ranking = df.select(['model','capacity_bytes']).distinct()

    #Define quantiles to create the buckets/rinking
    #Define the probabilities of the desire quantiles
    quantile_probabilities = [0.0, 0.25, 0.5, 0.75, 1.0]
    # Specify the relative error for approximation (0 for exact quantiles, but can be slow)
    relative_error = 0.01
    # Calculate the approximate quantiles
    quantiles = df_storage_ranking.approxQuantile('capacity_bytes', quantile_probabilities, relative_error)
    

    df_storage_ranking = df_storage_ranking.withColumn(
        'capacity_ranking',
        F.when(F.col('capacity_bytes') >= quantiles[3], "Huge")
        .when(F.col('capacity_bytes') >= quantiles[2], "Large")
        .when(F.col('capacity_bytes') >= quantiles[1], "Medium")
        .otherwise("Small")
    )

    # windowSpec = Window.orderBy(F.desc('capacity_bytes'))
    # df_storage_ranking = df_storage_ranking.withColumn('capacity_ranking', F.ntile(4).over('capacity_bytes'))

    df_join = df.join(df_storage_ranking, df['model'] == df_storage_ranking['model'], 'left')

    df_result = df_join.drop(df_storage_ranking.model).drop(df_storage_ranking.capacity_bytes)

    print("============================= STORAGE RANKING BUCKETS ADDED =============================")
    return df_result

def add_primary_key(df):
    """
    Check which combination of columns makes each row unique and create a new column with the value hash
    """
    print("============================= ADDING PRIMARY KEY COLUMN =============================")
    #Create a list os possible columns to be used as primary key
    pk_columns = ['serial_number','model','capacity_bytes']

    for i in range(1,len(pk_columns)+1):
        df_pk_col_check=df.groupBy(pk_columns[:i]).agg(F.count(F.col('serial_number')).alias('n_rows')).filter(F.col('n_rows')>1)
        if df_pk_col_check.count()==0 and len(pk_columns[:i])==1:
            df = df.withColumn('primary_key',F.hash(pk_columns[0]))
            break
        elif df_pk_col_check.count()==0 and len(pk_columns[:i])==2:
            df = df.withColumn('primary_key',F.hash(pk_columns[0],pk_columns[1]))
            break
        elif df_pk_col_check.count()==0 and len(pk_columns[:i])==3:
            df = df.withColumn('primary_key',F.hash(pk_columns[0],pk_columns[1],pk_columns[2]))
            break
    print("============================= PRIMARY KEY COLUMN ADDED =============================")
    return df

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    
    zip_file_paths=[]
    for file in os.listdir(ZIP_FOLDER):
        if file.endswith('.zip'):
            zip_file_path = os.path.join(ZIP_FOLDER,file)
            zip_file_paths.append(zip_file_path)

    #create RDD of each zip file content
    rdd_of_csv_contents=process_zip_files(spark,zip_file_paths)
    #merge all RDDs into one
    combined_rdd=spark.sparkContext.union(rdd_of_csv_contents)
    df = spark.read.csv(combined_rdd, header=True, inferSchema=True)

    print("============================= DATASET PRE-PROCESED =============================")
    df.select('date','serial_number','model','capacity_bytes').show()
    df = add_new_columns(zip_file_paths,df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)
    print("============================= DATASET AFTER PROCESSING =============================")
    df.select('date','serial_number','model','capacity_bytes','source_file','file_date','brand','capacity_ranking','primary_key').show()


if __name__ == "__main__":
    main()
