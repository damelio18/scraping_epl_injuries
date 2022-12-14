# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Transformation
import pandas as pd
import numpy as np
#from scraping_epl_injuries.airflow_dags.Functions.function_clean_date import clean_date

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------

# 1. Log the start of the DAG
def start_DAG():
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER INJURIES')

# 2. Load injuries data
def stg_table():
    # Data Lake credentials
    dl_pg_hook = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )
    # Connect to data lake
    dl_pg_conn = dl_pg_hook.get_conn()
    dl_cursor = dl_pg_conn.cursor()

    sql_statement = "SELECT player, dob, height, nationality, int_caps," \
                    "int_goals, current_club, season, injury, date_from," \
                    "date_until, days, games_missed FROM historical_injuries;"

    # Execute SQL statement
    result = dl_cursor.execute(sql_statement)

    return result


# # 2. Load injuries data
# def get_injuries():
#
#     # Data Lake credentials
#     dl_pg_hook = PostgresHook(
#         postgres_conn_id='datalake1_airflow',
#         schema='datalake1'
#     )
#
#     # SQL Statement
#     sql_statement = "SELECT player, dob, height, nationality, int_caps," \
#                     "int_goals, current_club, season, injury, date_from," \
#                     "date_until, days, games_missed FROM historical_injuries;"
#
#     # Connect to data lake
#     dl_pg_conn = dl_pg_hook.get_conn()
#     dl_cursor = dl_pg_conn.cursor()
#
#     # Execute SQL statements
#     dl_cursor.execute(sql_statement)
#
#     # Fetch all data from table
#     tuples_list = dl_cursor.fetchall()
#
#     # ----------------------------- Create DataFrame -----------------------------
#     # Create DataFrame
#     column_names = ['player', 'dob', 'height', 'nationality', 'int_caps',
#                     'int_goals', 'current_club', 'season', 'injury',
#                     'date_from', 'date_until','days', 'games_missed']
#
#     injuries_df_1 = pd.DataFrame(tuples_list, columns = column_names)
#
#     # ----------------------------- Transformation -----------------------------
#     #Save new instance of DataFrame
#     injuries_df_2 = injuries_df_1
#
#     # Test reformat
#     injuries_df_2['height'] = injuries_df_2['height'].replace('188', "blabla")
#
#     # Replace the empty strings and '-'
#     injuries_df_2 = injuries_df_2.replace(['NA'], np.nan)
#     injuries_df_2['date_until'] = injuries_df_2['date_until'].replace(['-'], np.nan)
#     injuries_df_2['games_missed'] = injuries_df_2['games_missed'].replace(['?', '-'], "0").astype('float')
#     injuries_df_2[['int_caps', 'int_goals']] = injuries_df_2[['int_caps', 'int_goals']].fillna('0')
#
#     # Revert DataFrame to list
#     injuries_df_2 = injuries_df_2.values.tolist()
#
#     injuries_df_2 = injuries_df_2[:50]
#
#     # ----------------------------- Load to Staging Table -----------------------------
#     # SQL Statements: Create, truncate and insert into staging table
#     sql_create_table = "CREATE TABLE IF NOT EXISTS injuries_stage (player VARCHAR(255), dob VARCHAR(255), " \
#                        "height VARCHAR(255), nationality VARCHAR(255), int_caps VARCHAR(255)," \
#                        "int_goals VARCHAR(255), current_club VARCHAR(255),season VARCHAR(255)," \
#                        " injury VARCHAR(255),date_from VARCHAR(255), date_until VARCHAR(255), " \
#                        " days VARCHAR(255), games_missed VARCHAR(255));"
#
#     sql_truncate_table = "TRUNCATE TABLE injuries_stage"
#
#     sql_add_data_to_table = """INSERT INTO injuries_stage (player, dob, height, nationality, \n
#                                                             int_caps, int_goals, current_club, season, \n
#                                                             injury, date_from, date_until, days, games_missed)
#                                VALUES ( %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s) """
#
#     # Create and truncate staging table
#     dl_cursor.execute(sql_create_table)
#     dl_cursor.execute(sql_truncate_table)
#
#     # Insert data into staging table
#     dl_cursor.executemany(sql_add_data_to_table, injuries_df_2)
#     dl_pg_conn.commit()
#     print(dl_cursor.rowcount, "Records inserted successfully into table")
#
#     return injuries_df_2



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
create_stg_table = PythonOperator(
    task_id = "create_stg_table",
    python_callable = stg_table,
    dag = dag
)

# .... End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> create_stg_table >> end_task