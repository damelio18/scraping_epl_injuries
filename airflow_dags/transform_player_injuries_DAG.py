# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from scraping_epl_injuries.airflow_dags.Functions.function_clean_date import clean_date

# Connecting to the Data Lake
#from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER INJURIES')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

dag = DAG('scrape_player_injuries_DAG',
          schedule_interval = '0 06 * * *',
          catchup = False,
          default_args = default_args)

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)


# ----------------------------- Trigger Tasks -----------------------------

start_task