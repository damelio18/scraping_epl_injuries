import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def team_urls:
    logging.info('Hello_TEST')


dag = DAG(
    'scrape_team_urls_DAG',
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

team_urls_task = PythonOperator(
    task_id="team_urls_task",
    python_callable=team_urls,
    dag = dag
)

team_urls_task