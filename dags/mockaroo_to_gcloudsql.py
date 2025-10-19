from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv
#from scripts.cc_data_pipeline import extract_cc_data
import os
import sys
import requests
import json
from io import StringIO
import pandas as pd

def say_api(api_key):
    print(f'API Key: {api_key}')

def test_function():
    print("File exists")

def extract_cc_data(api_key: str):
    print(api_key)
    """Extracts credit card data from Mockaroo API using the provided API key
    Returns a Pandas Dataframe
    """
    params = { "key" : api_key}
    headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {params.get("key")}'
    }

    response = requests.get('https://my.api.mockaroo.com/sample_cc_data_alex.json',headers=headers, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API Extraction failed status code {response.status_code}")

    json_string = json.dumps(response.json())
    json_object = StringIO(json_string)
    data = pd.read_json(json_object)
    data.to_csv('raw_output_cc_data.csv',index = True)

load_dotenv(dotenv_path='/home/alexdcruz011/first_etl_project/scripts/.env')

file_path = '/home/alexdcruz011/first_etl_project/scripts/raw_output_cc_data.csv'

default_args = {
    'owner': 'Alexander',
    'start_date': datetime(2025,10,19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ETL_mockaroo_to_gcs',
    default_args=default_args,
    schedule='*/10 * * * *',
    catchup=False,
    tags=['Cloud Storage','file_sensor']
) as dag:
    
    what_api = PythonOperator(
        task_id = 'what_is_my_api', 
        python_callable=say_api, 
        op_kwargs= {'api_key': os.getenv('API_KEY')})

    extract_mockaroo = PythonOperator(
        task_id = 'extract_mockaroo', 
        python_callable=extract_cc_data, 
        op_kwargs= {'api_key': os.getenv('API_KEY')}
    )

    file_check = FileSensor(
        task_id = 'sense_the_file',
        filepath=file_path,
        poke_interval=30,
        timeout=600,
        mode='poke'
    )

    print_it = PythonOperator(
        task_id = 'print_output',
        python_callable=test_function
    )

    what_api >> extract_mockaroo >> file_check >> print_it