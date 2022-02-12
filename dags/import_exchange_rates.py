"""This dag only runs some simple tasks to test Airflow's task execution."""
from datetime import datetime, timedelta
import os

from sqlalchemy import create_engine
import requests as r
from requests.exceptions import RequestException

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


conn_string = os.getenv('AIRFLOW__CORE__SQL_ALCHEMY_CONN')

BASE_URL = 'https://api.exchangerate.host/'


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(2)
}


dag = DAG(
    dag_id='my_dag',
    schedule_interval='5 * * * *',
    default_args=default_args
)


def get_convert_url(currency_from: str = 'BTC', currency_to: str = 'USD') -> str:
    return f'{BASE_URL}convert?from={currency_from}&to={currency_to}'


def get_rates():
    url = get_convert_url('BTC', 'USD')
    response = r.get(url)

    if response.status_code != 200:
        raise RequestException(response=response)

    data = response.json()
    print(data)


start_op = DummyOperator(
    task_id='start',
    dag=dag
)

get_rates_task = PythonOperator(
    python_callable=get_rates,
    task_id='get_rates',
    dag=dag
)

start_op >> get_rates_task