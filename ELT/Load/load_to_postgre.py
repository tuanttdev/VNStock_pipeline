import psycopg2
from ELT.Extract.extract import *
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import time
from utilities.utilities import get_windows_host_ip


def connect_to_postgres():
    WINDOWS_HOST = get_windows_host_ip()
    conn = psycopg2.connect(host=WINDOWS_HOST, dbname='BackendStock', user='postgres', password='tuantt', port='5432')
    if conn is not None:
        print('Postgres connected')
    else:
        print('Error connecting to PostgreSQL')
    return conn

def create_engine_postgres():
    WINDOWS_HOST = get_windows_host_ip()
    engine = create_engine(f'postgresql+psycopg2://postgres:tuantt@{WINDOWS_HOST}:5432/BackendStock')

    return engine



def delete_table(table_name=""):
    conn = connect_to_postgres()
    if conn is not None:
        conn.cursor().execute(f"delete from {table_name}")
        conn.commit()
        conn.close()

def load_data_company_to_postgres():
    conn = connect_to_postgres()
    if conn is not None:
        try:
            df = extract_company_list()
            cursor = conn.cursor()
            # psycopg2 không có hàm riêng dành cho dataframe nên cần convert từ df sang list of tuple rồi dùng execute_value
            # chuyển df thành list of tuple
            records = df.to_records(index=False).tolist()
            # sql insert

            sqlStr = "insert into company (symbol, exchange , type, organ_short_name ,organ_name, group_symbol) values %s"

            execute_values(cursor, sqlStr, records)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(e)

def get_column_of_table(table_name=""):

    conn = connect_to_postgres()
    cols_name = []
    if conn is not None:
        cursor = conn.cursor()

        cursor.execute(f"""SELECT column_name
                            FROM information_schema.columns
                            WHERE  table_name = '{table_name}'; """)
        cols_name = cursor.fetchall()
        cursor.close()
        conn.close()

    return cols_name

def load_data_price_board_to_postgres():
    # engine = create_engine_postgres()

    conn = connect_to_postgres()

    if conn is not None:
        # try:
        # get the cols name of price_board table
        cursor = conn.cursor()
        cols_name = get_column_of_table('price_board')
        ls_cols_name = [col[0] for col in cols_name]

        # print(ls_cols_name)

        # symbols = pd.read_sql("SELECT symbol FROM company WHERE exchange != 'DELISTED'", con=engine)
        cursor.execute(f"SELECT symbol FROM company WHERE exchange != 'DELISTED'")
        symbols = cursor.fetchall()

        df = extract_price_board(symbols)


        # psycopg2 không có hàm riêng dành cho dataframe nên cần convert từ df sang list of tuple rồi dùng execute_value
        # Flatten MultiIndex nếu có

        df.columns = ['_'.join([str(i) for i in col if str(i) != 'nan' ]) for col in df.columns]
        df = df[[col for col in df.columns if col in ls_cols_name]]
        cols = df.columns.tolist()


        # chuyển df thành list of tuple
        records = df.to_records(index=False).tolist()

        # sql insert
        sqlStr = f"""insert into price_board ( {','.join(cols)}) values %s"""

        execute_values(cursor, sqlStr, records)
        conn.commit()

        cursor.close()
        conn.close()
        print("Insert successful")

        # except Exception as e:
        #     logging.error(e)

def load_data_OHLCVT_to_postgres():
    try:
        t =datetime.now().time()

        if time(11, 30) <= t <= time(12, 0):
            print(" Skipping (outside 9:00–11:30)")
            return
        else:
            print('running task')

    except Exception as e:
        print(e)

    conn = connect_to_postgres()
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute(f"SELECT symbol FROM company WHERE exchange != 'DELISTED' and group_symbol ='VN100' ")
        companies = cursor.fetchall()

        sqlStr = "insert into OHLCVT_history (time , open_price , high, low , close_price , volume, symbol ) values %s"
        cols_mapping = {
            'time' : 'time',
            'open' : 'open_price',
            'high': 'high',
            'low': 'low',
            'close': 'close_price',
            'volume': 'volume'

        }

        for company in companies:
            print(company[0])

            try:
                df = extract_OHLCVT_history(symbol=company[0])
                df.rename(columns=cols_mapping, inplace=True)
                df['symbol'] = company[0]
                records = df.to_records(index=False).tolist()

                converted_records = [
                    (pd.to_datetime(row[0], unit='ns'), *row[1:])
                    for row in records
                ]

                print(converted_records)
                execute_values(cur=cursor, sql=sqlStr, argslist=converted_records)
                conn.commit()

            except Exception as e:
                logging.warning(f"{e} for symbol: {company[0]}")

        cursor.close()
        conn.close()

def load_matching_data_to_postgres(symbol=""):
    try:
        t = datetime.now().time()
        if time(11, 30) <= t <= time(12, 0):
            print(" Skipping (outside 9:00–11:30)")
            return
        else:
            print('running task')

    except Exception as e:
        print(e)


    conn = connect_to_postgres()
    if conn is not None:
        cursor = conn.cursor()
        # cursor.execute(f"SELECT symbol FROM company WHERE exchange != 'DELISTED' and group_symbol ='VN100'  ")
        # companies = cursor.fetchall()

        sqlStr = "insert into matching_trans (time , price , volume, match_type, id , symbol, last_modified) values %s on conflict (id) do nothing"


        # for company in companies:

            # try:

        df = extract_matching_data(symbol=symbol , last_time=None)

        df['symbol'] = symbol

        df['last_modified'] = datetime.now()
        records = df.to_records(index=False).tolist()

        converted_records = [
            (pd.to_datetime(row[0], unit='ns'), *row[1:])
            for row in records
        ]

        print(converted_records)
        execute_values(cur=cursor, sql=sqlStr, argslist=converted_records)
        conn.commit()

            # except Exception as e:
            #     logging.warning(f"{e} for symbol: {company[0]}")

        cursor.close()
        conn.close()


def update_company() :
    conn = connect_to_postgres()
    if conn is not None:
        cursor = conn.cursor()
        sqlStr = f"update company set group_symbol = 'VN100' WHERE symbol = ANY(%s)  "

        df = extract_symbol_by_group()
        records = df.tolist()
        cursor.execute(sqlStr, (records,))

        conn.commit()
        cursor.close()
        conn.close()

# delete_table('price_board')
load_matching_data_to_postgres(symbol ="PVI")
# update_company()
