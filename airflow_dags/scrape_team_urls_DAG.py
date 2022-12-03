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

    # Download content of url
    source = requests.get(url, headers=headers)

    # Parse html into BS4 object
    soup = BeautifulSoup(source.content, 'html.parser')
    print("soupy")

    # Find team urls and names
    for div in soup.findAll('td', attrs={'class': "hauptlink no-border-links"}):
        info = div.find_all('a')
        for a in info:

            # Extract team names
            substring = '.png'
            if substring in str(a.contents):
                pass
            else:
                n = 2
                team_name.append(str(a.contents)[n:-n])

            # Extract team urls
            if str(a['href']).rsplit("/", 2)[0] == "#":
                pass
            else:
                team_url.append("https://www.transfermarkt.com" + str(a['href']).rsplit("/", 2)[0])

    print(team_url)




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