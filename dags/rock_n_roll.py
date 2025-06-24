from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_rock_n_roll():
    print("🎸 Rock-n-roll baby!")
 

with DAG(
    dag_id='rock_n_roll_dag',
    start_date=datetime(2025, 6, 20),
    schedule_interval='@once',
    catchup=False,
    description='DAG "Rock-n-roll"',
    tags=['example'],
) as dag:


    rock_task = PythonOperator(
        task_id='say_rock_n_roll',
        python_callable=say_rock_n_roll,
    )

    rock_task
