from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Exercise_1.main import DivvyDataDownloader
from Exercise_2.main import scape_local_climatological_data


urls = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # invalid URL for testing
    ]

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    dag_id='DATA_ENGINEER_EXERCISE',
    description='A simple DAG',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

Bai1 = download_task = PythonOperator(
    task_id='download_files',
    python_callable=DivvyDataDownloader,
    op_kwargs={'urls': urls},
    dag=dag
)

Bai2 = PythonOperator(
    task_id='scaping_data',
    python_callable=scape_local_climatological_data,
    op_kwargs={'urls':"https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/",
            'target_date':"2024-01-19 10:27"},  
    dag=dag
)

Bai1 >> Bai2