# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Scraping
import requests
from bs4 import BeautifulSoup

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER URLS')

# 2. Get team URLS
def team_urls():
    # Empty list for team urls
    team_urls = []

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )

    # SQL Statement
    sql_statement = "SELECT team_url FROM team_urls"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_statement)

    # Extract team URLs from Data Lake
    for row in cursor.fetchall():
        team_urls.append(row[0])

    return team_urls

# 3. Scraping
def player_urls(ti):
    # get data returned from 'scrape_team_urls_task'
    data = ti.xcom_pull(task_ids = ['get_team_urls_task'])
    data = data[0][0]

    print("NOW")
    print(data)

# ----------------------------- Create DAG -----------------------------
dag = DAG(
    'scrape_player_urls_DAG',
    schedule_interval = '@daily',
    start_date = datetime.datetime.now() - datetime.timedelta(days=1))

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

# 2. Retrieve team urls from data lake
get_team_urls_task = PythonOperator(
    task_id = "get_team_urls_task",
    python_callable = team_urls,
    do_xcom_push = True,
    dag = dag
)

# 3. Scraping
scrape_player_urls_task = PythonOperator(
    task_id = "scrape_player_urls_task",
    python_callable = player_urls,
    dag = dag
)



# ----------------------------- Trigger Tasks -----------------------------

start_task >> get_team_urls_task >> scrape_player_urls_task