import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def greet():
	logging.info('Hello World Engineers!')

dag = DAG(
    'team_urls_dag',
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag = dag
)


greet_task