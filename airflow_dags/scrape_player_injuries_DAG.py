# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Scraping
import requests
from bs4 import BeautifulSoup
from function_player_bios import player_bio
from function_player_stats import table_data

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER INJURIES')

# 2. Get player URLS
def player_urls():
    # Empty list for player urls
    player_urls = []

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )

    # SQL Statement
    sql_statement = "SELECT player_url FROM player_urls"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_statement)

    # Extract player URLs from Data Lake
    for row in cursor.fetchall():
        player_urls.append(row[0])

    return player_urls

# 3. Start the scraping
def scrape_injuries(ti):
    # get data returned from 'get_player_urls_task'
    player_urls_xcom = ti.xcom_pull(task_ids = ['get_player_urls_task'])
    if not player_urls_xcom:
        raise ValueError('No value currently stored in XComs')

    # Extract player urls from nested list
    player_urls_xcom = player_urls_xcom[0]
    print(player_urls_xcom[:3])


# ----------------------------- Create DAG -----------------------------
dag = DAG(
    'scrape_player_injuries_DAG',
    schedule_interval = '@daily',
    start_date = datetime.datetime.now() - datetime.timedelta(days=1))

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

# 2. Retrieve player urls from data lake
get_player_urls_task = PythonOperator(
    task_id = "get_player_urls_task",
    python_callable = player_urls,
    do_xcom_push = True,
    dag = dag
)

# 3. Scraping
scrape_player_injuries_task = PythonOperator(
    task_id = "scrape_player_injuries_task",
    python_callable = scrape_injuries,
    #do_xcom_push = True,
    dag = dag
)


# ----------------------------- Trigger Tasks -----------------------------

start_task >> get_player_urls_task >> scrape_player_injuries_task