from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import numpy as np
import pandas as pd


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_fiat_data")
    
    id = []
    name = []
    min_size = []


    for i in data.values():
        for j in i:
            id.append(j['id'])
            name.append(j['name'])
            min_size.append(j['min_size'])


    transformed_data = {
        'Id' : id, 
        'Name' : name, 
        'Min_size' : min_size
    }


    transformed_data_list = transformed_data
    df_data = pd.DataFrame(transformed_data_list)

    aws_credentials = {"key": "XXXXXXX", "secret": "XXXXXXXXXX", "token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}


    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_fiat_coins_' + dt_string
    df_data.to_csv(f"s3://crypto-data-coinbase/{dt_string}.csv", index=False, storage_options=aws_credentials)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,1,3), # please look in case of any error
    'email': ['parkramuk@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)

}


with DAG('Fiat_dag',
    default_args=default_args,
    description='Cryptocurrency',
    schedule_interval='@daily',
    catchup = False) as dag:

    is_fiat_api_ready = HttpSensor(
    task_id = "is_fiat_api_ready",
    http_conn_id = "coinbase_api",
    endpoint= "/v2/currencies",
    )

    extract_fiat_data = SimpleHttpOperator(
    task_id = 'extract_fiat_data',
    http_conn_id = 'coinbase_api',
    endpoint= "/v2/currencies",
    method = 'GET',
    response_filter= lambda r: json.loads(r.text),
    log_response=True
    )

    transform_load_fiat_data = PythonOperator(
    task_id= 'transform_load_fiat_data',
    python_callable=transform_load_data
    )

    is_fiat_api_ready >> extract_fiat_data >> transform_load_fiat_data



