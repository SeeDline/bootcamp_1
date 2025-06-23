from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def adios_world():
    print("ничего не понятно, но очень интересно")

dag = DAG(
    dag_id='four_fours',
    description='вроде даг',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

bootcamp = PythonOperator(
    task_id='offer_for_350k',  # допустимый task_id
    python_callable=hello_world,
    dag=dag
)

bootcamp