
import os
import sys
from datetime import datetime , time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from ELT.Load.load_to_postgre import load_data_company_to_postgres , load_data_OHLCVT_to_postgres , load_matching_data_to_postgres, connect_to_postgres


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


with DAG(
    dag_id='load_data_matching_to_postgres',
    default_args=default_args,
    schedule_interval='*/1 9-11,13-14 * * 1-5',
    # schedule_interval = 'manual',
    catchup=False,
    max_active_runs=1
) as dag3:
    def get_symbols_from_db():
        conn = connect_to_postgres()
        symbols = []
        if conn is not None:
            cursor = conn.cursor()
            cursor.execute(f"SELECT symbol FROM company WHERE exchange != 'DELISTED' --and (group_symbol ='VN100' or group_symbol is null)  ")
            companies = cursor.fetchall()
            symbols = [row[0] for row in companies]
            cursor.close()

        conn.close()

        return symbols


    symbols = get_symbols_from_db()
    @task
    def call_for_matching_trans(symbol):
        print(f'{symbol}')
        try:
            t = datetime.now().time()
            print(t)
            if time(11, 30) <= t <= time(12, 0):
                print(" Skipping (outside 9:00–11:30)")
                return
            else:
                print('running task')
                load_matching_data_to_postgres(symbol=symbol)

        except Exception as e:
            print(e)
        # Gọi API vnstock theo symbol


    # Dynamic task mapping
    call_for_matching_trans.expand(symbol=symbols)
