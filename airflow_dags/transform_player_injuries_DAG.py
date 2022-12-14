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
    logging.info('STARTING THE DAG,OBTAINING EPL PLAYER INJURIES')

# 2. Create staging table
def stg_table():
    # ----------------------------- Get Data from Data Lake -----------------------------
    # Data lake credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )
    # Connect to data lake
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement: Get data from data lake
    sql_statement_get_data = "SELECT player, dob, height, nationality, int_caps," \
                             "int_goals, current_club, season, injury, date_from," \
                             "date_until, days, games_missed FROM historical_injuries;"

    # Fetch all data from table in data lake
    cursor_1.execute(sql_statement_get_data)
    tuples_list = cursor_1.fetchall()

    tuples_list = tuples_list[45:51]

    # ----------------------------- Create Staging Table in Data Warehouse -----------------------------
    # Data warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='test_dw',
        schema='test_dw'
    )
    # Connect to data warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement: Drop staging table
    sql_statement_drop = "DROP TABLE stg_historical_injuries"

    # SQL Statement: Create staging table
    sql_statement_create_table = "CREATE TABLE IF NOT EXISTS stg_historical_injuries (player VARCHAR(255)," \
                                 "dob VARCHAR(255), height VARCHAR(255), nationality VARCHAR(255), " \
                                 "int_caps VARCHAR(255), int_goals VARCHAR(255), current_club VARCHAR(255)," \
                                 "season VARCHAR(255), injury VARCHAR(255),date_from VARCHAR(255), " \
                                 "date_until VARCHAR(255), days VARCHAR(255), games_missed VARCHAR(255));"

    # Create and insert data into data warehouse staging table
    cursor_2.execute(sql_statement_drop)
    cursor_2.execute(sql_statement_create_table)
    for row in tuples_list:
        cursor_2.execute('INSERT INTO stg_historical_injuries VALUES %s', (row,))
    pg_conn_2.commit()

# 3. Missing Values
def missing_values():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='test_dw',
        schema='test_dw'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement: Get data from staging data
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch all data from table
    tuples_list = dw_cursor.fetchall()

    # ----------------------------- Create DataFrame -----------------------------
    # Create DataFrame
    column_names = ['player', 'dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # ----------------------------- Transformation -----------------------------
    # Replace the empty strings and '-'
    df = df.replace(['NA'], np.nan)
    df['date_until'] = df['date_until'].replace(['-'], np.nan)
    df['games_missed'] = df['games_missed'].replace(['?', '-'], "0").astype('float')
    df[['int_caps', 'int_goals']] = df[['int_caps', 'int_goals']].fillna('0')

    # ----------------------------- Load to Staging Table -----------------------------
    # SQL Statement: Truncate staging table
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Truncate staging table
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)

# 4. Player names
def player_names():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='test_dw',
        schema='test_dw'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement: Get data from staging data
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch all data from table
    tuples_list = dw_cursor.fetchall()

    # ----------------------------- Create DataFrame -----------------------------
    # Create DataFrame
    column_names = ['player', 'dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # ----------------------------- Transformation -----------------------------
    # Strip all leading and trailing whitespace from player names
    df['player'] = df.player.str.strip()

    # Split name into first and second name
    df[['first_name', 'second_name']] = df['player'].str.split(' ', n=1, expand=True)

    # Remove players with no first name
    df = df[~df['first_name'].isnull()]

    # Drop player column
    df = df.drop(['player'], axis=1)

    # ----------------------------- Load to Staging Table -----------------------------
    # SQL Statement: Truncate staging table
    sql_alter_1 = "ALTER TABLE stg_historical_injuries ADD first_name VARCHAR(255)"
    sql_alter_2 = "ALTER TABLE stg_historical_injuries ADD second_name VARCHAR(255)"
    sql_alter_3 = "ALTER TABLE stg_historical_injuries DROP COLUMN player;"

    # SQL Statement: Truncate staging table
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Truncate staging table
    dw_cursor.execute(sql_alter_1)
    dw_cursor.execute(sql_alter_2)
    dw_cursor.execute(sql_alter_3)
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)


# 5. Clean dates
def date_columns():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='test_dw',
        schema='test_dw'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement: Get data from staging data
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch all data from table
    tuples_list = dw_cursor.fetchall()

    # ----------------------------- Create DataFrame -----------------------------
    # Create DataFrame
    column_names = ['dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed',
                    'first_name', 'second_name']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # ----------------------------- Transformation -----------------------------
    # # Standarise format of all date columns to ('mmm dd, yyy')
    df['dob'] = [v[:-4] + " " + v[-4:] for v in df['dob']]
    df['dob'] = [v[:3] + " " + v[3:] for v in df['dob']]

    # Clean dob column
    clean_date(df, 'dob')

    # Create Age column: Days difference
    df['age'] = date.today() - df['dob']

    # Create Age column: Convert age to years
    df['age'] = round(df['age'] / np.timedelta64(1, 'Y'), 0)

    # Clean date_from column
    clean_date(df, 'date_from')

    # Clean date_until column
    clean_date(df, 'date_until')

    return list(df.columns)


# ----------------------------- Load to Staging Table -----------------------------
#     # SQL Statement: Truncate staging table
#     sql_alter = "ALTER TABLE stg_historical_injuries ADD dob_day VARCHAR(255)," \
#                   "ADD dob_mon VARCHAR(255),ADD dob_year VARCHAR(255),ADD age VARCHAR(255)," \
#                   "ADD date_from_day VARCHAR(255), ADD date_until_day VARCHAR(255)"
#
#     # SQL Statement: Truncate staging table
#     sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"
#
#     # Truncate staging table
#     dw_cursor.execute(sql_alter)
#     dw_cursor.execute(sql_truncate_table)
#     dw_pg_conn.commit()
#
#     # Create a list of tuples representing the rows in the dataframe
#     rows = [tuple(x) for x in df.values]
#
#     # Insert the rows into the database
#     dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)













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
stg_table_task = PythonOperator(
    task_id = "stg_table_task",
    python_callable = stg_table,
    dag = dag
)

# 3. Missing Values
missing_values_task = PythonOperator(
    task_id = "missing_values_task",
    python_callable = missing_values,
    dag = dag
)

# 4. Player names
player_names_task = PythonOperator(
    task_id = "player_names_task",
    python_callable = player_names,
    dag = dag
)

# 5. Dates
clean_dates_task = PythonOperator(
    task_id = "clean_dates_task",
    python_callable = date_columns,
    dag = dag
)




# .... End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> stg_table_task >> missing_values_task >> player_names_task >> clean_dates_task >> end_task