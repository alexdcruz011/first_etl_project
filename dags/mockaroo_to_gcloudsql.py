import sys
sys.path.append('/home/alexdcruz011/first_etl_project')
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from dotenv import load_dotenv
from scripts.cc_data_pipeline import extract_cc_data
import os
import pandas as pd
from pandera import Column, DataFrameSchema
import pandera as pa
import google.auth
import gcsfs

raw_gcs_path = 'gs://raw_data_alex_portfolio/raw_data/raw_output_cc_alex.csv'

def transform_mockaroo_data():
    """Function reads Extracts raw_output_cc data from GCS then performs transformation and schema check before saving a new output which will be uploaded to GCS as final data"""
    credentials, project = google.auth.default()
    fs = gcsfs.GCSFileSystem(token=credentials)
    with fs.open(raw_gcs_path) as f:
        df = pd.read_csv(f)
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
    df.to_csv('scripts/output_cc_data.csv')


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(DAG_DIR)
ENV_PATH = os.path.join(PARENT_DIR,'scripts','.env')
load_dotenv(dotenv_path=ENV_PATH)

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

    # Calls extract_cc_data from the scripts directory with python file cc_data_pipeline.py
    extract_mockaroo = PythonOperator(
        task_id = 'extract_mockaroo', 
        python_callable=extract_cc_data, 
        op_kwargs= {'api_key': os.getenv('API_KEY'), 'gcp_path': raw_gcs_path}
    )

    # Calls transform_mockaroo_data function defined in code above to extract and transform raw data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_mockaroo_data
    )

    # Uploads output from transform_mockaroo_data to GCS
    upload_final_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_final_data',
        src=os.path.join(PARENT_DIR,'scripts','output_cc_data.csv'),
        dst='raw_data/output_cc_alex.csv',
        bucket='raw_data_alex_portfolio',
        mime_type='text/csv',
        gcp_conn_id='google_cloud_default'
    )


    extract_mockaroo >> upload_raw_to_gs >> transform_data >> upload_final_to_gcs
