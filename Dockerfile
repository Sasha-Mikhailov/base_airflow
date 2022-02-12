ARG PYTHON_VERSION=3.9
ARG AIRFLOW_VERSION=2.2.3

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

ADD requirements.txt .

RUN apt-get update && apt-get install default-libmysqlclient-dev libmariadb-dev g++ -yqq
RUN pip install -r ./requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

USER airflow
