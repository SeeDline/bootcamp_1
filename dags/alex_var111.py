from airflow.operators.python import PythonOperator

def print_hello():
    print("Как же я заебался комитит эту хуйню")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)
