from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def fetch_hh_vacancies(**kwargs):
    """Загрузка вакансий с HH API для Дагестана"""
    url = "https://api.hh.ru/vacancies"
    params = {
        'text': 'Дагестан OR Махачкала',
        'area': 1,  # Россия
        'per_page': 100,  # Максимальное количество на странице
        'page': 0
    }
    
    all_vacancies = []
    while True:
        response = requests.get(url, params=params)
        data = response.json()
        all_vacancies.extend(data.get('items', []))
        
        if params['page'] >= data.get('pages', 1) - 1:
            break
        params['page'] += 1

    # Сохраняем сырые данные
    os.makedirs('/data/vacancies', exist_ok=True)
    raw_path = f"/data/vacancies/hh_dagestan_{datetime.now().strftime('%Y-%m-%d')}.json"
    with open(raw_path, 'w') as f:
        json.dump(all_vacancies, f)
    
    kwargs['ti'].xcom_push(key='raw_vacancies_path', value=raw_path)
    return len(all_vacancies)

def process_vacancies(**kwargs):
    """Обработка вакансий"""
    ti = kwargs['ti']
    raw_path = ti.xcom_pull(task_ids='fetch_vacancies', key='raw_vacancies_path')
    
    with open(raw_path, 'r') as f:
        vacancies = json.load(f)
    
    processed_data = []
    for v in vacancies:
        processed_data.append({
            'vacancy_id': v['id'],
            'name': v['name'],
            'employer': v['employer']['name'],
            'salary_from': v['salary']['from'] if v['salary'] else None,
            'salary_to': v['salary']['to'] if v['salary'] else None,
            'currency': v['salary']['currency'] if v['salary'] else None,
            'published_at': v['published_at'],
            'url': v['alternate_url'],
            'experience': v['experience']['name'],
            'employment': v['employment']['name']
        })
    
    df = pd.DataFrame(processed_data)
    csv_path = f"/data/vacancies/processed_hh_dagestan_{datetime.now().strftime('%Y-%m-%d')}.csv"
    df.to_csv(csv_path, index=False)
    ti.xcom_push(key='processed_vacancies_path', value=csv_path)
    return df.shape[0]

def load_to_postgres(**kwargs):
    """Загрузка в PostgreSQL"""
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='process_vacancies', key='processed_vacancies_path')
    df = pd.read_csv(csv_path)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Создаем таблицу если не существует
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS hh_dagestan_vacancies (
        vacancy_id VARCHAR(20) PRIMARY KEY,
        name TEXT,
        employer TEXT,
        salary_from NUMERIC,
        salary_to NUMERIC,
        currency VARCHAR(3),
        published_at TIMESTAMP,
        url TEXT,
        experience VARCHAR(50),
        employment VARCHAR(50),
        load_date DATE DEFAULT CURRENT_DATE
    );
    """
    with engine.connect() as conn:
        conn.execute(create_table_sql)
    
    # Загружаем данные
    df.to_sql(
        'hh_dagestan_vacancies',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )

with DAG(
    'hh_dagestan_vacancies',
    default_args=default_args,
    description='Ежедневная загрузка вакансий по Дагестану с HH',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hh', 'dagestan', 'vacancies'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_vacancies',
        python_callable=fetch_hh_vacancies,
    )

    process_task = PythonOperator(
        task_id='process_vacancies',
        python_callable=process_vacancies,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    check_data = PostgresOperator(
        task_id='check_data_quality',
        postgres_conn_id='postgres_conn',
        sql="""
        SELECT 
            COUNT(*) as total_vacancies,
            MIN(published_at) as oldest_date,
            MAX(published_at) as newest_date
        FROM hh_dagestan_vacancies
        WHERE load_date = CURRENT_DATE;
        """,
    )

    fetch_task >> process_task >> load_task >> check_data