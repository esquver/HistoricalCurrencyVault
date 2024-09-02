from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Numeric, Date
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1993, 3, 12),
    'retries': 48,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'currency_rates_parser',
    default_args=default_args,
    description='Парсинг курсов валют и запись в PostgreSQL',
    schedule_interval='0 15 * * *',
)

def get_pg_connection() -> str:
    """
    Получение строки подключения к PostgreSQL.
    
    Входные данные:
        - Нет
    
    Выходные данные:
        - Строка подключения к PostgreSQL
    """
    conn = BaseHook.get_connection('postgres_conn')
    return f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

def parse_data(**kwargs) -> None:
    """
    Парсинг данных о курсах валют с сайта cbr.ru.
    
    Входные данные:
        - execution_date: Дата выполнения задачи
    
    Выходные данные:
        - JSON с данными о курсах валют
    """
    execution_date: datetime = kwargs['execution_date']
    date_str: str = execution_date.strftime('%d.%m.%Y')
    url: str = f"https://cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={date_str}"
    try:
        response: requests.Response = requests.get(url)
        response.raise_for_status()
        table: BeautifulSoup = BeautifulSoup(response.text, "html.parser").find("table")
        df: pd.DataFrame = pd.read_html(str(table).replace(',', '.'))[0]
        df.insert(0, "Дата", date_str)
        kwargs['ti'].xcom_push(key='parsed_data', value=df.to_json())
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе данных: {e}")
    except Exception as e:
        logging.error(f"Ошибка при парсинге данных: {e}")

def rename_header(**kwargs) -> None:
    """
    Переименование заголовков столбцов в DataFrame.
    
    Входные данные:
        - JSON с данными о курсах валют
    
    Выходные данные:
        - JSON с переименованными заголовками столбцов
    """
    data_json: str = kwargs['ti'].xcom_pull(key='parsed_data')
    df: pd.DataFrame = pd.read_json(data_json)
    translations: dict = {
        'Дата': 'date',
        'Цифр. код': 'digital_code',
        'Букв. код': 'letter_code',
        'Единиц': 'measurement',
        'Валюта': 'currency',
        'Курс': 'rate'
    }
    df = df.rename(columns=translations)
    kwargs['ti'].xcom_push(key='renamed_data', value=df.to_json())

def store_currencies(**kwargs) -> None:
    """
    Сохранение данных о валютах в PostgreSQL.
    
    Входные данные:
        - JSON с переименованными заголовками столбцов
    
    Выходные данные:
        - Нет
    """
    data_json: str = kwargs['ti'].xcom_pull(key='renamed_data')
    df: pd.DataFrame = pd.read_json(data_json)
    pg_con: str = get_pg_connection()
    engine = create_engine(pg_con)
    
    metadata = MetaData(schema='finance')
    currencies_table = Table('currencies', metadata,
                             Column('digital_code', Integer, primary_key=True),
                             Column('letter_code', String),
                             Column('currency', String),
                             Column('measurement', Integer),
                             autoload_with=engine)
    
    df['digital_code'] = pd.to_numeric(df['digital_code'], errors='coerce').fillna(-1).astype(int)
    df['letter_code'] = df['letter_code'].fillna('-1')
    df = df.drop_duplicates(subset=['digital_code'])
    
    invalid_rows: pd.DataFrame = df[(df['digital_code'] == -1) & (df['letter_code'] == '-1')]
    if not invalid_rows.empty:
        logging.warning(f"Найдены недопустимые значения в столбце letter_code: {invalid_rows}")
    
    currencies_df: pd.DataFrame = df[['digital_code', 'letter_code', 'currency', 'measurement']]
    records: list = currencies_df.to_dict(orient='records')
    
    insert_stmt = insert(currencies_table).values(records)
    do_update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['digital_code'],
        set_={
            'letter_code': insert_stmt.excluded.letter_code,
            'currency': insert_stmt.excluded.currency,
            'measurement': insert_stmt.excluded.measurement
        }
    )
    
    with engine.connect() as conn:
        conn.execute(do_update_stmt)

def store_exchange_rates(**kwargs) -> None:
    """
    Сохранение данных о курсах валют в PostgreSQL.
    
    Входные данные:
        - JSON с переименованными заголовками столбцов
    
    Выходные данные:
        - Нет
    """
    data_json: str = kwargs['ti'].xcom_pull(key='renamed_data')
    df: pd.DataFrame = pd.read_json(data_json)
    pg_con: str = get_pg_connection()
    engine = create_engine(pg_con)
    
    metadata = MetaData(schema='finance')
    exchange_rates_table = Table('exchange_rates', metadata,
                                 Column('date', Date, primary_key=True),
                                 Column('digital_code', Integer, primary_key=True),
                                 Column('rate', Numeric(10, 4)),
                                 autoload_with=engine)
    
    df['digital_code'] = pd.to_numeric(df['digital_code'], errors='coerce').fillna(-1).astype(int)
    df = df.drop_duplicates(subset=['date', 'digital_code'])
    
    if df['rate'].dtype == 'object':
        df['rate'] = df['rate'].str.replace(' ', '').astype(float)
    else:
        df['rate'] = df['rate'].astype(float)
    
    invalid_rows: pd.DataFrame = df[(df['digital_code'] == -1) | (df['rate'].isna())]
    if not invalid_rows.empty:
        logging.warning(f"Найдены недопустимые значения в столбцах digital_code и rate: {invalid_rows}")
    
    exchange_rates_df: pd.DataFrame = df[['date', 'digital_code', 'rate']]
    records: list = exchange_rates_df.to_dict(orient='records')
    
    insert_stmt = insert(exchange_rates_table).values(records)
    do_update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['date', 'digital_code'],
        set_={'rate': insert_stmt.excluded.rate}
    )
    
    with engine.connect() as conn:
        conn.execute(do_update_stmt)

parse_data_task = PythonOperator(
    task_id='parse_data_task',
    python_callable=parse_data,
    provide_context=True,
    dag=dag,
)

rename_header_task = PythonOperator(
    task_id='rename_header_task',
    python_callable=rename_header,
    provide_context=True,
    dag=dag,
)

store_currencies_task = PythonOperator(
    task_id='store_currencies_task',
    python_callable=store_currencies,
    provide_context=True,
    dag=dag,
)

store_exchange_rates_task = PythonOperator(
    task_id='store_exchange_rates_task',
    python_callable=store_exchange_rates,
    provide_context=True,
    dag=dag,
)

parse_data_task >> rename_header_task >> store_currencies_task >> store_exchange_rates_task