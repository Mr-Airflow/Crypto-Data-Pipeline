from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import numpy as np
import pandas as pd


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_coin_data")
    
    asset_id = []
    code = []
    name = []
    color = []
    type = []
    sort_index = []
    exponent = []

    for d in data.values():
        for i in d:
            asset_id.append(i['asset_id'])
            code.append(i['code'])
            name.append(i['name'])
            color.append(i['color'])
            type.append(i['type'])
            sort_index.append(i['sort_index'])
            exponent.append(i['exponent'])


    transformed_data = {
        'Asset_id' : asset_id, 
        'Code' : code, 
        'Name' : name, 
        'Color' : color, 
        'Type' : type, 
        'sort_index' : sort_index, 
        'exponent' : exponent
    
    }


    transformed_data_list = transformed_data
    df_data = pd.DataFrame(transformed_data_list)

    aws_credentials = {"key": "XXXXXXX", "secret": "XXXXXXXXXX", "token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}


    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_listed_crypto_coins_' + dt_string
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


with DAG('Crypto_coins_dag',
    default_args=default_args,
    description='Cryptocurrency',
    schedule_interval='@daily',
    catchup = False) as dag:

    is_coin_api_ready = HttpSensor(
    task_id = "is_coinbase_api_ready",
    http_conn_id = "coinbase_api",
    endpoint= "/v2/currencies/crypto",
    )

    extract_coin_data = SimpleHttpOperator(
    task_id = 'extract_coin_data',
    http_conn_id = 'coinbase_api',
    endpoint= "/v2/currencies/crypto",
    method = 'GET',
    response_filter= lambda r: json.loads(r.text),
    log_response=True
    )

    transform_load_coin_data = PythonOperator(
    task_id= 'transform_load_coin_data',
    python_callable=transform_load_data
    )

    is_coin_api_ready >> extract_coin_data >> transform_load_coin_data



