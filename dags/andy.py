from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("WTF???")

dag = DAG(
    'wtf',
    description='Hello',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

hello_task = PythonOperator(
    task_id='hello',
    python_callable=hello_world,
    dag=dag
)