from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator

file_path = '/home/alexdcruz011/first_etl_project/scripts/output_cc_data.csv'

def process_file():
    with open('/home/alexdcruz011/first_etl_project/output.txt','w') as f:
        print('File was there', file=f)


default_args = {
    'owner': 'Alexander',
    'start_date': datetime(2025,10,10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='sample_dag_script',
    default_args=default_args,
    schedule='*/10 * * * *',
    catchup=False,
    tags=['example','file_sensor']
) as dag:

    wait_for_file = FileSensor(
        task_id = 'sense_the_file',
        filepath=file_path,
        poke_interval=30,
        timeout=600,
        mode='poke'
    )

    python_operator = PythonOperator(
        task_id='test_python_function',
        python_callable=process_file
    )

wait_for_file >> python_operator