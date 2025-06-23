from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def adios_world():
    print("ничего не понятно, но очень интересно")

dag = DAG(
    dag_id='GIT_ALMIGHTY_PUSH',
    description='Женя, Володя, спасибо за контент',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

bootcamp = PythonOperator(
    task_id='offer_for_100000k',  # допустимый task_id
    python_callable=adios_world,
    dag=dag
)

bootcamp