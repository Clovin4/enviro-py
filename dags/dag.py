from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG

from etl import Extract
from etl import Transform
from etl import Load

from sqlalchemy import create_engine


with DAG("etl", start_date=datetime(2023,8,3), schedule_interval="@daily", catchup=False) as dag:
    pass

    extract = PythonOperator(
        task_id="extract",
        python_callable=Extract().get_weather,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=Transform().transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=Load().load,
    )

    extract >> transform >> load
    