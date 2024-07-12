from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import numpy as np
import pandas as pd


def transform_load_data(task_instance):
    time_now = task_instance.xcom_pull(task_ids="extract_time_data")
    
    date = []
    epoch = []

    date.append(time_now['data']['iso'])
    epoch.append(time_now['data']['epoch'])

    transformed_data = {"Date-iso" : date, "Epoch" : epoch}

    df_data = pd.DataFrame(transformed_data)

    aws_credentials = {"key": "XXXXXXX", "secret": "XXXXXXXXXX", "token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}


    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_time_' + dt_string
    df_data.to_csv(f"s3://crypto-data-coinbase/{dt_string}.csv", index=False, storage_options=aws_credentials)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,1,7), 
    'email': ['parkramuk@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)

}


with DAG('Time_dag',
    default_args=default_args,
    description='time_dag',
    schedule_interval='@daily',
    catchup = False) as dag:

    extract_time_data = SimpleHttpOperator(
    task_id = 'extract_time_data',
    http_conn_id = 'coinbase_api',
    endpoint= "/v2/time",
    method = 'GET',
    response_filter= lambda r: json.loads(r.text),
    log_response=True
    )

    transform_load_time_data = PythonOperator(
    task_id= 'transform_load_time_data',
    python_callable=transform_load_data
    )

    extract_time_data >> transform_load_time_data



