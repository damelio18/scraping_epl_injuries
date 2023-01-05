# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Transformation
import pandas as pd
import numpy as np

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG, CREATING TABLES FOR DW')

# 2. Join fpl and transfermarkt data
def join_data():

    ################ Get injuries data from DW

    # Data warehouse: injuries
    pg_hook_1 = PostgresHook(
        postgres_conn_id='dw_injuries',
        schema='dw_injuries'
    )
    # Connect to data warehouse: injuries
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement: Get data
    sql_statement_get_data = "SELECT * FROM store_player_bios;"

    # Fetch data
    cursor_1.execute(sql_statement_get_data)
    tuples_list_1 = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['code', 'first_name', 'second_name', 'current_club',
                    'dob_day', 'dob_mon', 'dob_year', 'dob', 'age', 'height',
                    'nationality', 'int_caps', 'int_goals', 'injury_risk']

    df1 = pd.DataFrame(tuples_list_1, columns = column_names)

    print(tuples_list_1)



# .... Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED, LOADED TO DW')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

# Schedule for 8am daily
dag = DAG('6_dw_performance_DAG',
          schedule_interval = '0 08 * * *',
          catchup = False,
          default_args = default_args)

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

# 2. Join data
join_data_task = PythonOperator(
    task_id = "join_data_task",
    python_callable = join_data,
    dag = dag
)

# .... End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> join_data_task >> end_task