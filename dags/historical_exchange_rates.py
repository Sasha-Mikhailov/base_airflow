import logging
from datetime import datetime, timedelta

import requests as r
from requests.exceptions import RequestException
from sqlalchemy import create_engine

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from common.meta import rates, create_table_if_not_exists
from common import CONN_STRING, BASE_URL, DT_FORMAT

logger = logging.getLogger()

HISTORICAL_PERIOD_LIMIT_DAYS = 365

TASK_ARGS = {
    'currency_from': Variable.get(key="currency_from", default_var='BTC'),
    'currency_to': Variable.get(key="currency_to", default_var='USD'),
    'start_date': Variable.get(key="start_date", default_var='1999-01-01'),
    'end_date': Variable.get(key="end_date", default_var=datetime.strftime(datetime.today(), DT_FORMAT)),
}

default_dag_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

dag = DAG(
    dag_id='get_historical_rates',
    description='load historical exchange rates',
    default_args=default_dag_args,
)


def get_historical_url():
    return BASE_URL + 'timeseries'


def get_historical_request_params(
        start_date: str,
        end_date: str,
        base: str,
        symbols: str,  # or list of strs
        source: str = 'crypto'):
    if not isinstance(symbols, list):
        symbols = [symbols]

    return {
        'start_date': start_date,
        'end_date': end_date,
        'base': base,
        'symbols': ','.join(symbols),
        'source': source,
    }


def get_rates(currency_from='BTC', currency_to='USD', start_date='1999-01-01', end_date='2021-01-31', ):
    url = get_historical_url()
    params = get_historical_request_params(
        base=currency_from,
        symbols=currency_to,
        start_date=start_date,
        end_date=end_date,
    )

    response = r.get(url, params=params)

    if response.status_code != 200:
        raise RequestException(response=response)

    logger.info(f'get response from {url}')
    return response.json()


def convert_data_from_response(data, currency_from, currency_to):
    records = data.get('rates')

    if not records:
        raise ValueError('rates in Response are empty')

    logger.info(f'Got {len(records)} values from API')
    logger.info(f'Example data: {[r for r in records.items()][1]}')
    utcnow = datetime.utcnow()

    result = [{
        'currency_from': currency_from,
        'currency_to': currency_to,
        'rate': record[currency_to],
        'date': date,
        'utc_created_dttm': utcnow,
    } for date, record in records.items() if currency_to in record]

    logger.info(f'Converted: {len(result)} items')

    return result


def load_data(result, start_date, end_date):
    if not result:
        raise ValueError(f'result is empty: {result}')

    engine = create_engine(CONN_STRING)

    create_table_if_not_exists(engine, rates)

    delete_query = rates.delete().where(rates.c.date.between(start_date, end_date))

    with engine.begin() as transaction:
        # delete data for same period for the sake of idempotency
        transaction.execute(delete_query)
        logger.info(f'deleted previous data: {delete_query}')

        query = rates.insert()
        logger.info(f'prepared query: {query}')

        res = transaction.execute(query, result)
        logger.info(f'inserted data ({res})')


def get_list_of_period_ranges(start_date, end_date):
    start_dt = datetime.strptime(start_date, DT_FORMAT)
    end_dt = datetime.strptime(end_date, DT_FORMAT)

    if (end_dt - start_dt).days < HISTORICAL_PERIOD_LIMIT_DAYS:
        return [(start_date, end_date)]

    logger.info(f'period between {start_dt} and {end_date} is more than limit ({HISTORICAL_PERIOD_LIMIT_DAYS} days). Iterating')
    periods = []
    period_start = start_dt
    period_end = start_dt

    while period_end < end_dt:
        period_end = min(period_start + timedelta(days=HISTORICAL_PERIOD_LIMIT_DAYS), end_dt)
        periods.append(
            (datetime.strftime(period_start, DT_FORMAT),
            datetime.strftime(period_end, DT_FORMAT),)
        )
        period_start = period_end + timedelta(days=1)

    logger.info(f'got {len(periods)} periods')
    return periods



def historical_etl(*arg, **kwargs):
    currency_from = kwargs.get('currency_from')
    currency_to = kwargs.get('currency_to')
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')

    periods = get_list_of_period_ranges(start_date, end_date)

    for period in periods:
        logger.info(f'\nRequesting rates for {currency_from}/{currency_to} for {period[0]}..{period[1]}')
        data = get_rates(currency_from, currency_to, period[0], period[1])

        result = convert_data_from_response(data, currency_from=currency_from, currency_to=currency_to)

        load_data(result, period[0], period[1])

    logger.info(f'\nFinished')


start_op = DummyOperator(
    task_id='start',
    dag=dag
)

get_rates_task = PythonOperator(
    python_callable=historical_etl,
    task_id='get_historical_rates_and_load_to_db',
    dag=dag,
    op_kwargs=TASK_ARGS,
)

start_op >> get_rates_task
