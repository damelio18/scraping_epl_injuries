# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Scraping
import requests
from bs4 import BeautifulSoup

# For Data Lake Upload
from airflow.hooks.postgres_hook import PostgresHook
#from airflow.operators.postgres_operator import PostgresOperator

# ----------------------------- Define Functions -----------------------------

def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL TEAM URLS')

def team_urls():
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

    #print(team_name)
    return team_name, team_url


def load(ti):
    data = ti.xcom_pull(task_ids = ['scrape_team_urls_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    data_team_name = data[0]
    data_team_url = data[1]

    #print(data_team_name)
    print("break")
    #print(data_team_url)

    #sql_truncate_table = 'TRUNCATE TABLE Team_URLs'
    sql_truncate_table = "SELECT * FROM Team_URLs"
    #sql_add_data_to_table = 'INSERT INTO Team_URLs (team_name, team_url) VALUES (%s, %s)'

    pg_hook = PostgresHook(
        postgres_conn_id = 'datalake1_airflow',
        schema = 'datalake1'
    )

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_truncate_table)

    # # Add data to table
    # for elem in zip(team_name, team_url):
    #     cursor.execute(sql_add_data_to_table, elem)
    #     pg_conn.commit()


    return cursor.fetchall()

    #print(data)

def finish_DAG():
    logging.info('DAG HAS FINISHED,OBTAINED EPL TEAM URLS')

# ----------------------------- Create DAG -----------------------------
dag = DAG(
    'scrape_team_urls_DAG',
    schedule_interval = '@hourly',
    start_date = datetime.datetime.now() - datetime.timedelta(days=1))

# ----------------------------- Set Tasks -----------------------------

start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

scrape_team_urls_task = PythonOperator(
    task_id = "scrape_team_urls_task",
    python_callable = team_urls,
    do_xcom_push = True,
    dag = dag
)

load_to_data_lake_task = PythonOperator(
    task_id = "load_to_data_lake_task",
    python_callable = load,
    dag = dag
)

end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> scrape_team_urls_task >> load_to_data_lake_task >> end_task