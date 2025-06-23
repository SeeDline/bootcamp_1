from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("ЯХАААА БАЛЯЯЯЯ")

dag = DAG(
    dag_id='yyaaaaha_balaaa__this_is_Murad_Legenda',
    description='Где Ваши Даги, я того трубу шатал! Эти ДЕшники уже попутали',
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