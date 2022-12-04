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

# 3. Start the scraping
def player_urls(ti):
    # get data returned from 'get_team_urls_task'
    team_urls_xcom = ti.xcom_pull(task_ids = ['get_team_urls_task'])
    if not team_urls_xcom:
        raise ValueError('No value currently stored in XComs')

    # Extract team urls from nested list
    team_urls_xcom = team_urls_xcom[0]

    # Empty lists to add player names and urls
    player_name = []
    player_url = []
    player_counter = 0

    # Headers required to scrape Transfermarkt
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    for i in team_urls_xcom:
        # Download content of url
        source = requests.get(i, headers=headers)

        # Parse html into BS4 object
        soup = BeautifulSoup(source.content, 'html.parser')

        # Find and extract team urls
        for div in soup.find_all('span', attrs={'class': "show-for-small"}):
            info = div.find_all('a')
            for a in info:
                # Get player name
                n = 2
                player_name.append(str(a.contents)[n:-n])

                # Get player url and change to injury section
                s = str(a['href'].rsplit("/", 3)[0])
                t = "/verletzungen/"
                u = str(a['href'].split("/", 3)[3])
                player_url.append("https://www.transfermarkt.com" + s + t + u)

            player_counter += 1
            if (player_counter % 50) == 0:
                print(str(player_counter) + " Player URLs Scraped")

    print("Scraping Completed: " + str(len(player_url)) + " Player URLs obtained")


    #print(team_urls_xcom)
    return player_name, player_url

# 4. Load scraping data to the data lake
def load(ti):
    # get data returned from 'scrape_player_urls_task'
    data = ti.xcom_pull(task_ids=['scrape_player_urls_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Separate team name and team url
    data_player_name = data[0][0]
    data_player_url = data[0][1]

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )

    # SQL statements: Drop, create and insert into table
    sql_drop_table = "DROP TABLE player_URLs;"
    sql_create_table = "CREATE TABLE IF NOT EXISTS player_urls (player_url_id SERIAL NOT NULL, player_name VARCHAR(255), player_url VARCHAR(255), upload_time timestamp DEFAULT CURRENT_TIMESTAMP);"
    sql_add_data_to_table = 'INSERT INTO player_urls (player_name, player_url) VALUES (%s, %s)'

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_drop_table)
    cursor.execute(sql_create_table)

    # Add data to table
    for elem in zip(data_player_name, data_player_url):
        cursor.execute(sql_add_data_to_table, elem)
        pg_conn.commit()

    print("Successfully loaded data to the data lake")


# 5. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,OBTAINED EPL TEAM URLS')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

dag = DAG('scrape_player_urls_DAG',
          schedule_interval = '00 05 * * *',
          catchup = False,
          default_args = default_args)

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
    do_xcom_push = True,
    dag = dag
)

# 4. Load to data lake
load_to_data_lake_task = PythonOperator(
    task_id = "load_to_data_lake_task",
    python_callable = load,
    dag = dag
)

# 5. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> get_team_urls_task >> scrape_player_urls_task >> load_to_data_lake_task >> end_task