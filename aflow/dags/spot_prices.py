from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import numpy as np
import pandas as pd


def transform_load_data(task_instance):
    spot_prices = task_instance.xcom_pull(task_ids="extract_spot_prices_data")
    
    amount = []
    base = []
    currency = []

    i = spot_prices['data']

    for j in i:
        amount.append(j['amount'])
        base.append(j['base'])
        currency.append(j['currency'])
        
    transformed_data = {'amount' : amount, 'base' : base, 'currency' : currency}

    df_data = pd.DataFrame(transformed_data)

    aws_credentials = {"key": "XXXXXXX", "secret": "XXXXXXXXXX", "token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}


    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_spot_prices_' + dt_string
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


with DAG('Spot_prices_dag',
    default_args=default_args,
    description='spot_prices_dag',
    schedule_interval='@daily',
    catchup = False) as dag:

    is_spot_prices_api_ready = HttpSensor(
    task_id = "is_spot_prices_api_ready",
    http_conn_id = "coinbase_api",
    endpoint= "/v2/prices/USD/spot",
    )

    extract_spot_prices_data = SimpleHttpOperator(
    task_id = 'extract_spot_prices_data',
    http_conn_id = 'coinbase_api',
    endpoint= "/v2/prices/USD/spot",
    method = 'GET',
    response_filter= lambda r: json.loads(r.text),
    log_response=True
    )

    transform_load_spot_prices_data = PythonOperator(
    task_id= 'transform_load_spot_prices_data',
    python_callable=transform_load_data
    )

    is_spot_prices_api_ready >> extract_spot_prices_data >> transform_load_spot_prices_data



