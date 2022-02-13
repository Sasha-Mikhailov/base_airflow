import os
import logging
from datetime import datetime, timedelta

import requests as r
from requests.exceptions import RequestException
from sqlalchemy import create_engine, MetaData

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from common.meta import rates, create_table_if_not_exists
from common import CONN_STRING, BASE_URL, DT_FORMAT


logger = logging.getLogger()


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='get_historical_rates',
    description='load historical exchange rates',
    default_args=default_args,
    catchup=False,
)


def get_historical_url() -> str:
    if BASE_URL[-1] != '/':
        BASE_URL += '/'

    return BASE_URL + 'timeseries'


def get_historical_request_params(
        start_date: str = '1999-01-01',
        end_date: str = '2021-01-31',
        base: str = 'BTC',
        symbols: list = ['USD'],
        source: str = 'crypto'):
    """

    :rtype: dict
    """
    if not isinstance(symbols, list):
        symbols = [symbols]

    return {
        'start_date': start_date,
        'end_date': end_date,
        'base': base,
        'symbols': ','.join(symbols),
        'source': source,
    }


def conver_data_from_response(data, currency_from = 'BTC', currency_to = 'USD'):
    if not data.get('success'):
        raise ValueError

    utcnow = datetime.utcnow()

    result = [{
        'currency_from': base_currency,
        'currency_to': currency_to,
        'rate': record[currency_to],
        'date': date,
        'utc_updated_dttm': utcnow,
    } for date, record in data['rates'].items()]

    return result


def get_rates(currency_from = 'BTC', currency_to = 'USD'):
    url = get_historical_url()
    params = get_historical_request_params(
        base=currency_from,
        symbols=[currency_to],
    )

    response = r.get(url, params=params)

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
    currency_from = 'BTC'
    currency_to = 'USD'

    data = get_rates(currency_from, currency_to)

    result = conver_data_from_response(
        data,
        currency_from=currency_from,
        currency_to=currency_to
    )

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
