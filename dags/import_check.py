from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from scripts.cc_data_pipeline import extract_cc_data

def test_import():
    print("Import works!")

with DAG('test_import_dag', start_date=datetime(2025, 10, 19), schedule_interval=None, catchup=False) as dag:
    task = PythonOperator(task_id='test_import', python_callable=test_import)