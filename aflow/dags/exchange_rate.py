from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import numpy as np
import pandas as pd


def transform_load_data(task_instance):
    exchange_rates = task_instance.xcom_pull(task_ids="extract_exchange_data")
    
    e_rate = exchange_rates['data']['rates']
    e_curr = exchange_rates['data']['currency']

    currency = []
    name = []
    rate = []


    for key, value in e_rate.items():
        name.append(key)
        rate.append(value)
        currency.append("USD")

    transformed_data = {
        'Name' : name, 
        'Rate' : rate, 
        'Currency' : currency
    }

    transformed_data_list = transformed_data
    df_data = pd.DataFrame(transformed_data_list)

    aws_credentials = {"key": "XXXXXXX", "secret": "XXXXXXXXXX", "token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}


    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_exchange_rates_' + dt_string
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


with DAG('Exchange_rate_dag',
    default_args=default_args,
    description='Cryptocurrency',
    schedule_interval='@daily',
    catchup = False) as dag:


    extract_exchange_data = SimpleHttpOperator(
    task_id = 'extract_exchange_data',
    http_conn_id = 'coinbase_api',
    endpoint= "/v2/exchange-rates",
    method = 'GET',
    response_filter= lambda r: json.loads(r.text),
    log_response=True
    )

    transform_load_exchange_data = PythonOperator(
    task_id= 'transform_load_exchange_data',
    python_callable=transform_load_data
    )

    extract_exchange_data >> transform_load_exchange_data



