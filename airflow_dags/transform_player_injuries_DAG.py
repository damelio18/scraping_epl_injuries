# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Transformation
import pandas as pd
#from scraping_epl_injuries.airflow_dags.Functions.function_clean_date import clean_date

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER INJURIES')

# 2. Load injuries data
def get_injuries():
    # ----------------------------- Get Injuries -----------------------------
    # Data Lake credentials
    dl_pg_hook = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )

    # SQL Statement
    sql_statement = "SELECT team_url, team_name FROM team_urls;"

    # Connect to data lake
    dl_pg_conn = dl_pg_hook.get_conn()
    dl_cursor = dl_pg_conn.cursor()

    # Execute SQL statements
    dl_cursor.execute(sql_statement)

    # Fetch all data from table
    tuples_list = dl_cursor.fetchall()

    # ----------------------------- Create DataFrame -----------------------------
    # Create DataFrame
    column_names = ['one', 'two']
    injuries_df_1 = pd.DataFrame(tuples_list, columns = column_names)

    # ----------------------------- Transformation -----------------------------
    # Test reformat
    injuries_df_1['two'] = injuries_df_1['two'].replace('Chelsea FC', "blabla")

    # Revert DataFrame to list
    injuries_df_2 = injuries_df_1.values.tolist()

    # ----------------------------- Load to Staging Table -----------------------------
    # SQL Statements: Create staging table and insert into staging table
    sql_create_table = "CREATE TABLE IF NOT EXISTS test_stage (one VARCHAR(255), two VARCHAR(255));"
    sql_add_data_to_table = """INSERT INTO test_stage (one, two)
                               VALUES (%s, %s) """
    # Create table
    dl_cursor.execute(sql_create_table)

    # Insert data into staging table
    dl_cursor.executemany(sql_add_data_to_table, injuries_df_2)
    dl_pg_conn.commit()
    print(dl_cursor.rowcount, "Records inserted successfully into table")

    return injuries_df_2

# .... Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,OBTAINED EPL PLAYER INJURIES')

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
    python_callable = get_injuries,
    dag = dag
)

# .... End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> get_injuries_task >> end_task