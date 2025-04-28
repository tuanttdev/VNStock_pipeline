import os
import sys
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ELT.Load.load_to_postgre import  load_data_company_to_postgres , load_data_OHLCVT_to_postgres , load_matching_data_to_postgres


default_args = {
    'owner': 'Thanh Tuan',
    'start_date': datetime(2024, 4, 14)

}

dag = DAG(
    dag_id='load_company_data_into_postgre',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
)

upload_postgre = PythonOperator(
    task_id='upload_postgre',
    python_callable=load_data_company_to_postgres,
    dag=dag
)


dag2 = DAG(
    dag_id='load_data_OHLCVT_to_postgres',
    default_args=default_args,
    schedule_interval='*/1 9-11,13-15 * * 1-5',
    catchup=False
)

upload_postgre = PythonOperator(
    task_id='upload_postgre',
    python_callable=load_data_OHLCVT_to_postgres,
    dag=dag2
)


dag3 = DAG(
    dag_id='load_data_matching_to_postgres',
    default_args=default_args,
    schedule_interval='*/1 9-11,13-15 * * 1-5',
    catchup=False
)

upload_postgre = PythonOperator(
    task_id='upload_postgre',
    python_callable=load_matching_data_to_postgres,
    dag=dag3
)
