from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def programmer_joke():
    print("Why do programmers prefer dark mode?")
    print("Because light attracts bugs!")

dag = DAG(
    'programmer_joke_dag',
    description='DAG that tells a programmer joke',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

joke_task = PythonOperator(
    task_id='tell_joke',
    python_callable=programmer_joke,
    dag=dag
)