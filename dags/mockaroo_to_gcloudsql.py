import sys
sys.path.append('/home/alexdcruz011/first_etl_project')
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv
from scripts.cc_data_pipeline import extract_cc_data
import os
import pandas as pd
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from pandera import Column, DataFrameSchema
import pandera as pa

def transform_mockaroo_data():
    schema = DataFrameSchema({
        'first_name' : Column(str),
        'last_name' : Column(str),
        'email' : Column(str),
        'gender' : Column(str),
        'credit_card_number' : Column(str),
        'mobile_number' : Column(str,nullable=True),
        'mailing_address' : Column(str,nullable=True),
        'ssn' : Column(str,nullable=True),
        'date_of_birth' : Column(pa.DateTime,nullable=True)
    })

    df = pd.read_csv(file_path)
    df['first_name'] = df['first_name'].str.capitalize()
    df['last_name'] = df['last_name'].str.capitalize()
    df['email'] = df['email'].fillna('no_email@email.com')
    df['remaining_balance'] = df['remaining_balance'].astype(float)
    df['ssn'] = df['ssn'].str[-4:]
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
    df['gender'] = df['gender'].fillna('Not Specified')
    df['credit_card_number'] = df['credit_card_number'].astype(str)

    df.rename(columns={'remaining_balance':'credit_limit'},inplace=True)
    schema.validate(df)
    df.to_csv('scripts/output_cc_data.csv', index= False)


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(DAG_DIR)
ENV_PATH = os.path.join(PARENT_DIR,'scripts','.env')
load_dotenv(dotenv_path=ENV_PATH)

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

    upload_raw_to_gs = LocalFilesystemToGCSOperator(
        task_id='upload_raw_data',
        src=os.path.join(PARENT_DIR,'scripts','raw_output_cc_data.csv'),
        dst='raw_data/raw_output_cc_alex.csv',
        bucket='raw_data_alex_portfolio',
        mime_type='text/csv',
        gcp_conn_id='google_cloud_default'
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_mockaroo_data
    )

    upload_final_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_final_data',
        src=os.path.join(PARENT_DIR,'scripts','output_cc_data.csv'),
        dst='raw_data/output_cc_alex.csv',
        bucket='raw_data_alex_portfolio',
        mime_type='text/csv',
        gcp_conn_id='google_cloud_default'
    )


    what_api >> extract_mockaroo >> upload_raw_to_gs >> transform_data >> upload_final_to_gcs