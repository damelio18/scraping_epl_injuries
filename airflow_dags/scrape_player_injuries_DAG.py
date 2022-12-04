# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Functions for Scraping
import requests
from bs4 import BeautifulSoup
from scraping_epl_injuries.airflow_dags.Functions.function_player_bios import player_bio
from scraping_epl_injuries.airflow_dags.Functions.function_player_stats import table_data

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

    # Empty lists to insert player data
    ## NB stats1 is for player with one injury page and stats2 for players with multiple injury pages
    stats1 = []
    stats2 = []

    # Counter for progress of scraping
    counter = 0

    # Headers required to scrape Transfermarkt
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    # Loop through each player url
    for url in player_urls_xcom[:2]:

        # ----------------------------- Obtain html data -----------------------------
        # Get content of url
        source = requests.get(url, headers=headers)

        # Parse html into BS4 object
        soup = BeautifulSoup(source.content, 'html.parser')

        # Search for number of injury pages
        test_pages = soup.find_all('ul', attrs={'class': "tm-pagination"})

        # ----------------------------- Scrape players with x1 injury page -----------------------------
        # If player has one injury page
        if len(test_pages) == 0:

            # Get player biography
            player_bios = player_bio(soup)

            # Add injury history to player_biography
            player_injury_history = table_data(soup, player_bios)

            # Table data
            dataframe_stats_1 = player_injury_history[0]

            # Table headers
            dataframe_headers = player_injury_history[1]

            # Extend table data with previous players in the loop
            stats1.extend(dataframe_stats_1)

        # ----------------------------- Scrape players with multiple injury pages -----------------------------

        # If player has multiple injury pages
        else:
            # Get url's for each injury table
            for div in test_pages:
                info = div.find_all('a')
                num_pages = len(info) - 2

                # Iterate through the different injury pages
                while num_pages > 0:
                    # Create new urls for each injury page
                    new_url = str(url) + "/page/" + str(num_pages)

                    # Get content of url
                    source = requests.get(new_url, headers=headers)

                    # Parse html into BS4 object
                    soup = BeautifulSoup(source.content, 'html.parser')

                    # Get player biography
                    player_bios = player_bio(soup)

                    # Add injury history to player_biography
                    player_injury_history = table_data(soup, player_bios)

                    # Table data
                    dataframe_stats_2 = player_injury_history[0]

                    # Table headers
                    dataframe_headers = player_injury_history[1]

                    # Extend table data with previous players in the loop
                    stats2.extend(dataframe_stats_2)

                    # Scrape next injury page for player
                    num_pages = num_pages - 1

        # Print progress of scraping
        counter += 1
        if (counter % 50) == 0:
            print(str(counter) + " Players Scraped")

    print("Successfully Scraped: " + str(counter) + " Players")

    # Join injury and bio data for all players
    stats_joined = stats1 + stats2

    return stats_joined




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