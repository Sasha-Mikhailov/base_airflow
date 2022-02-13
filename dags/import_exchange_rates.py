import os
import logging
from datetime import datetime

import requests as r
from requests.exceptions import RequestException
from sqlalchemy import create_engine, MetaData

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from common import CONN_STRING, BASE_URL, DT_FORMAT
from common.meta import rates, create_table_if_not_exists


logger = logging.getLogger()


default_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='my_dag',
    # schedule_interval='5 * * * *',
    default_args=default_args
)


def get_convert_url(currency_from: str = 'BTC', currency_to: str = 'USD') -> str:
    return f'{BASE_URL}convert?from={currency_from}&to={currency_to}'


def get_data_from_response(data):
    if not data.get('success'):
        raise ValueError

    return {
        'currency_from': data['query']['from'],
        'currency_to': data['query']['to'],
        'rate': data['info']['rate'],
        'date': datetime.strptime(data['date'], DT_FORMAT),
        'utc_created_dttm': datetime.utcnow(),
        'utc_updated_dttm': datetime.utcnow(),
    }


def get_rates():
    url = get_convert_url('BTC', 'USD')
    response = r.get(url)

    if response.status_code != 200:
        raise RequestException(response=response)

    return response.json()


def load_data(result):
    engine = create_engine(CONN_STRING)

    create_table_if_not_exists(engine, rates)

    with engine.connect() as conn:
        # metadata_obj = MetaData(bind=conn)
        # rates.metadata = metadata_obj
        query = rates.insert()
        print(f'prepeared query: {query}')

        res = conn.execute(query, result)
        print(f'inserted data ({res})')


def etl():
    data = get_rates()

    result = get_data_from_response(data)

    load_data(result)


start_op = DummyOperator(
    task_id='start',
    dag=dag
)

get_rates_task = PythonOperator(
    python_callable=etl,
    task_id='get_rates_and_load_to_db',
    dag=dag
)

start_op >> get_rates_task
