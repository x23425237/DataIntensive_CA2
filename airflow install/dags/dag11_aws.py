from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
#https://www.youtube.com/watch?v=yPTjzv7JRec


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 15),
    'retries': 1,
}

# Define the function to load data
def load_csv_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = postgres_hook.get_sqlalchemy_engine()

    dd = '/opt/airflow/data/cardata_oscarf.csv'
    data = pd.read_csv(dd)
    
    data.to_sql('cardata_oscarf', engine, schema='formula1', if_exists='replace', index=False)
    print('Data inserted into PostgreSQL')


# Define the DAG
with DAG(
    dag_id='csv_to_postgres_multi_aws',
    default_args=default_args,
    start_date=datetime(2025, 4, 16),
    schedule_interval='@daily',  
) as dag:

    task_load_csv = PythonOperator(
        task_id='load_csv_to_postgres_driversAws',
        python_callable=load_csv_to_postgres
    )


# load another 

def load_season_csv_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = postgres_hook.get_sqlalchemy_engine()

    dd = '/opt/airflow/data/cardata_hamf.csv'
    data = pd.read_csv(dd)

    data.to_sql('cardata_hamf', engine, schema='formula1', if_exists='replace', index=False)
    print('Races data inserted into PostgreSQL')


task_load_seasons_csv = PythonOperator(
        task_id='load_csv_to_postgres_seasons',
        python_callable=load_season_csv_to_postgres
    )






task_load_csv >> task_load_seasons_csv 



