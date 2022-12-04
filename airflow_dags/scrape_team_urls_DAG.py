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
    logging.info('STARTING THE DAG,OBTAINING EPL TEAM URLS')


# 2. Start the scraping
def team_urls():
    # Empty lists to add team names and urls
    team_name = []
    team_url = []

    # Headers required to scrape Transfermarkt
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    # Url to scrape
    url = "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"

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

    return team_name, team_url


# 3. Load scraping data to the data lake
def load(ti):
    # get data returned from 'scrape_team_urls_task'
    data = ti.xcom_pull(task_ids = ['scrape_team_urls_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Separate team name and team url
    data_team_name = data[0][0]
    data_team_url = data[0][1]

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id = 'datalake1_airflow',
        schema = 'datalake1'
    )

    # SQL statements: Drop, create and insert into table
    sql_drop_table = "DROP TABLE Team_URLs;"
    sql_create_table = "CREATE TABLE IF NOT EXISTS Team_URLs (team_id SERIAL NOT NULL, team_name VARCHAR(255), team_url VARCHAR(255), upload_time timestamp DEFAULT CURRENT_TIMESTAMP);"
    sql_add_data_to_table = 'INSERT INTO Team_URLs (team_name, team_url) VALUES (%s, %s)'

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_drop_table)
    cursor.execute(sql_create_table)

    # Add data to table
    for elem in zip(data_team_name, data_team_url):
        cursor.execute(sql_add_data_to_table, elem)
        pg_conn.commit()

    print("Successfully loaded data to the data lake")


# 4. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,OBTAINED EPL TEAM URLS')

# ----------------------------- Create DAG -----------------------------
dag = DAG(
    'scrape_team_urls_DAG',
    schedule_interval = "27 13 * * *",
    start_date = datetime.datetime(2022,12,4, pytz.UTC))

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

# 2. Scraping
scrape_team_urls_task = PythonOperator(
    task_id = "scrape_team_urls_task",
    python_callable = team_urls,
    do_xcom_push = True,
    dag = dag
)

# 3. Load to data lake
load_to_data_lake_task = PythonOperator(
    task_id = "load_to_data_lake_task",
    python_callable = load,
    dag = dag
)

# 4. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> scrape_team_urls_task >> load_to_data_lake_task >> end_task