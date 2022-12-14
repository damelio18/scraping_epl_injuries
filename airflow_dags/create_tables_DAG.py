# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Transformation
import pandas as pd
import numpy as np
from datetime import date
from scraping_epl_injuries.airflow_dags.Functions.function_clean_date import clean_date

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG, CREATING TABLES FOR DW')


# 2. Create player bios table
def bios():
    # ----------------------------- Get Injuries Data from Data Warehouse -----------------------------
    # Data warehouse: injuries credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='dw_injuries',
        schema='injuries'
    )
    # Connect to data warehouse: injuries
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement: Get data data warehouse: injuries
    sql_statement_get_data = "SELECT 'first_name', 'second_name','current_club'," \
                             "'dob_day', 'dob_mon','dob_year', 'dob','age'," \
                             "'height', 'nationality','int_caps', 'int_goals';"

    # Fetch all data from table in data warehouse: injuries
    cursor_1.execute(sql_statement_get_data)
    tuples_list = cursor_1.fetchall()

    # ----------------------------- Create DataFrame -----------------------------
    # Create DataFrame
    column_names = ['first_name', 'second_name', 'current_club', 'dob_day',
                    'dob_mon','dob_year', 'dob', 'age', 'height',
                    'nationality', 'int_caps','int_goals']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Change types
    #df = df.astype({'dob_day': 'int', 'dob_mon': 'int', 'dob_year': 'int',
    #                'age': 'int', 'height': 'int', 'int_caps': 'int',
    #                'int_goals': 'int'})

    # ----------------------------- Load to Data Warehouse with other sources -----------------------------
    # Data warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='datawarehouse_airflow',
        schema='datawarehouse'
    )
    # Connect to data warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement: Drop old table
    sql_drop_table = "DROP TABLE IF EXISTS store_player_bios"

    # SQL Statement: Create new table
    sql_create_table = "CREATE TABLE IF NOT EXISTS store_player_bios (first_name VARCHAR(255)," \
                             "second_name VARCHAR(255), current_club VARCHAR(255), dob_day VARCHAR(255), " \
                             "dob_mon VARCHAR(255), dob_year VARCHAR(255), dob VARCHAR(255)," \
                             "age VARCHAR(255), height VARCHAR(255),nationality VARCHAR(255), " \
                             "int_caps VARCHAR(255), int_goals VARCHAR(255));"

    # Alter and truncate staging table
    cursor_2.execute(sql_drop_table)
    cursor_2.execute(sql_create_table)
    pg_conn_2.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    pg_hook_2.insert_rows(table="store_player_bios", rows=rows)

    return tuples_list


# 4. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,EPL PLAYER INJURIES & BIOS LOADED TO DW')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

dag = DAG('create_tables_DAG',
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

# 2. Start Task
create_bios_task = PythonOperator(
    task_id = "create_bios_task",
    python_callable = bios,
    dag = dag
)

# 4. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)



# ----------------------------- Trigger Tasks -----------------------------
start_task >> create_bios_task >> end_task
