import datetime
import logging

# For Scraping
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def team_urls():
    logging.info('Hello_TEST')

    # Empty lists to add team names and urls
    team_name = []
    team_url = []

    # Headers required to scrape Transfermarkt
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    # Url to scrape
    url = "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"
    print("YES,YES")
    print(url)




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