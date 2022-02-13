import os


CONN_STRING = os.getenv('AIRFLOW__CORE__SQL_ALCHEMY_CONN')

BASE_URL = 'https://api.exchangerate.host/'

DT_FORMAT = "%Y-%m-%d"
