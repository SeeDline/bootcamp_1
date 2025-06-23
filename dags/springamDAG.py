from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello World, i'm SPRINGAM")

dag = DAG(
    'TEST_DAG_for_bootcamp',
    description='Простой DAG для теста и проверки работы',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

hello_task = PythonOperator(
    task_id='springam_task',
    python_callable=hello_world,
    dag=dag
)
