# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from scraping_epl_injuries.airflow_dags.Functions.function_clean_date import clean_date

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER INJURIES')

# 2. Get player URLS
def load_injuries():

    # Data Lake credentials
    pg_hook = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )

    # SQL Statement
    sql_statement = "SELECT * FROM historical_injuries;"

    # Connect to data lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_statement)

    # Fetch all data from table
    tuples_list = cur.fetchall()

    # Column names for the DataFrame
    column_names = ['injury_id', 'transfermarkt_id', 'player', 'dob', 'height',
                    'nationality', 'int_caps', 'int_goals', 'current_club',
                    'shirt_number', 'season', 'injury', 'date_from', 'date_until',
                    'days', 'games_missed', 'scrape_time']

    # Create DataFrame
    injuries_df_1 = pd.DataFrame(tuples_list, columns = column_names)

    return injuries_df_1


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

dag = DAG('transform_player_injuries_DAG',
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

# 2. Retrieve player urls from data lake
get_injuries_task = PythonOperator(
    task_id = "get_injuries_task",
    python_callable = load_injuries,
    #do_xcom_push = True,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> get_injuries_task