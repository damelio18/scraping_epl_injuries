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


# 2. Assign fpl_player_id to transfermarkt.com players
def assign_ids():

    ################ Get injuries data from DW

    # Data warehouse credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT * FROM store_clean_historical_injuries;"

    # Execute SQL statement
    cursor_1.execute(sql_statement_get_data)

    # Fetch data
    tuples_list_1 = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['dob', 'height', 'nationality', 'int_caps', 'int_goals',
                    'team', 'season', 'injury', 'date_from','date_until',
                    'days_injured','games_missed', 'first_name', 'second_name',
                    'dob_day', 'dob_mon', 'dob_year', 'age','date_from_day',
                    'date_from_mon', 'date_from_year','date_until_day',
                    'date_until_mon','date_until_year']

    df1 = pd.DataFrame(tuples_list_1, columns = column_names)

    ################ Get fpl data from DW

    # Data warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id = 'fantasypl',
        schema='fantasypl'
    )
    # Connect to data warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT first_name, second_name, web_name, " \
                             "team, code FROM store_gameweeks;"

    # Execute SQL statement
    cursor_2.execute(sql_statement_get_data)

    # Fetch data
    tuples_list_2 = cursor_2.fetchall()

    # Create DataFrame
    column_names = ['first_name', 'second_name', 'web_name', 'team', 'code']

    df2 = pd.DataFrame(tuples_list_2, columns = column_names)

    # Remove duplicate rows
    df2 = df2.drop_duplicates()

    ################ Join datasets

    # Join 1 - based on first and second name
    df = pd.merge(df1, df2[['first_name', 'second_name', 'team', 'code']],
                        on=['first_name', 'second_name', 'team'], how='left')

    # Joined successfully
    assigned = df[df['code'].notnull()]

    # Joined unsuccessfully
    missing = df[df['code'].isnull()]
    missing.pop("code")

    # Join 2 - based on second_name and web_name
    missing = pd.merge(missing, df2[['web_name', 'team', 'code']],
                       left_on=['second_name', 'team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to master
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop("code")

    # Join 3 - based on first_name and web_name
    missing = pd.merge(missing, df2[['web_name', 'team', 'code']],
                       left_on=['first_name', 'team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to master
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Change duplicate matched players
    assigned.loc[assigned['first_name'].str[:] == 'Adama', 'code'] = '159533'
    assigned.loc[assigned['second_name'].str[:] == 'Santos Carneiro Da Cunha', 'code'] = '430871'

    ################ Load data to staging table

    # SQL Statements
    sql_drop_stage = "DROP TABLE IF EXISTS stage_clean_historical_injuries;"
    sql_statement_create_table = "CREATE TABLE IF NOT EXISTS stage_clean_historical_injuries (dob VARCHAR(255)," \
                                 "height VARCHAR(255), nationality VARCHAR(255), int_caps VARCHAR(255), " \
                                 "int_goals VARCHAR(255), team VARCHAR(255), season VARCHAR(255), " \
                                 "injury VARCHAR(255),date_from VARCHAR(255), date_until VARCHAR(255), " \
                                 "days_injured VARCHAR(255), games_missed VARCHAR(255), first_name VARCHAR(255)," \
                                 "second_name VARCHAR(255), dob_day VARCHAR(255), dob_mon VARCHAR(255)," \
                                 "dob_year VARCHAR(255), age VARCHAR(255), date_from_day VARCHAR(255), " \
                                 "date_from_mon VARCHAR(255), date_from_year VARCHAR(255), date_until_day VARCHAR(255)," \
                                 "date_until_mon VARCHAR(255), date_until_year VARCHAR(255), code VARCHAR(255));"

    # Execute SQL statements
    cursor_1.execute(sql_drop_stage)
    cursor_1.execute(sql_statement_create_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in assigned.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="stage_clean_historical_injuries", rows=rows)


# 3. Create player bios table
def bios():

    ################ Get data from staging table

    # Data warehouse credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT code, first_name, second_name, " \
                             "team, dob_day, dob_mon, dob_year," \
                             "dob, age, height, nationality, int_caps, " \
                             "int_goals, games_missed FROM stage_clean_historical_injuries;"

    # Execute SQL statement
    cursor_1.execute(sql_statement_get_data)

    # Fetch data
    tuples_list = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['code', 'first_name', 'second_name', 'team',
                    'dob_day', 'dob_mon','dob_year', 'dob', 'age', 'height',
                    'nationality', 'int_caps','int_goals','games_missed']

    df = pd.DataFrame(tuples_list, columns = column_names)

    ################ Create injury KPI

    # Deal with missing values
    df['games_missed'] = df['games_missed'].replace(['NaN'], 0.0)
    df = df.fillna("")

    # Change columns to numeric type
    change_type = ['age', 'games_missed']
    df[change_type] = df[change_type].apply(pd.to_numeric)

    # Sum games missed for each player in their career
    column_names = column_names[:-1]
    df = df.groupby(column_names, as_index=False)["games_missed"].sum()

    # Calculate games missed per season
    df['games_missed_per_season'] = round(df['games_missed'] / (df['age'] - 18), 0)

    # Create percentiles for injury risk
    df['injury_risk'] = round(df.games_missed_per_season.rank(pct=True) * 10).astype('string')
    df['injury_risk'] = df['injury_risk'].str.rsplit('.', 1).str.get(0)
    df.loc[df['code'] == "487117.0", "injury_risk"] = '0'

    # Drop unwanted columns
    df = df.drop(['games_missed', 'games_missed_per_season'], axis=1)

    # Change columns to numeric type
    change_type = ['code','dob_day','dob_mon','dob_year','height','int_caps','int_goals','injury_risk']
    df[change_type] = df[change_type].apply(pd.to_numeric)

    ################ Load data to dw_injuries

    # Data warehouse credentials for loading
    pg_hook_2 = PostgresHook(
        postgres_conn_id='dw_injuries',
        schema='dw_injuries'
    )
    # Connect to data warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statements
    sql_create_table = "CREATE TABLE IF NOT EXISTS store_player_bios (code int PRIMARY KEY," \
                       "first_name VARCHAR(255), second_name VARCHAR(255), current_club VARCHAR(255)," \
                       "dob_day int, dob_mon int, dob_year int," \
                       "dob date, age int, height int," \
                       "nationality VARCHAR(255), int_caps int, int_goals int, injury_risk int);"
    sql_truncate_table = "TRUNCATE TABLE store_player_bios"

    # Execute SQL statements
    cursor_2.execute(sql_create_table)
    cursor_2.execute(sql_truncate_table)
    pg_conn_2.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    pg_hook_2.insert_rows(table="store_player_bios", rows=rows)


# 4. Create historical injuries table
def injuries():

    ################ Get data from staging table

    # Data warehouse credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT code, season, injury, date_from_day," \
                             "date_from_mon, date_from_year, date_from, " \
                             "date_until_day, date_until_mon, date_until_year," \
                             "date_until, days_injured, games_missed " \
                             "FROM stage_clean_historical_injuries;"

    # Execute SQL statement
    cursor_1.execute(sql_statement_get_data)

    # Fetch data
    tuples_list = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['code', 'season', 'injury', 'date_from_day',
                    'date_from_mon', 'date_from_year', 'date_from',
                    'date_until_day', 'date_until_mon', 'date_until_year',
                    'date_until', 'days_injured', 'games_missed']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Remove players with no injury history
    df = df[df['season'].notna()]

    # Remove current injuries
    df = df[df['date_until'].notna()]

    # Change columns to int type
    change_int = ['code','date_from_day','date_from_mon','date_from_year','date_until_day',
                  'date_until_mon','date_until_year','days_injured', 'games_missed']
    df[change_int] = df[change_int].apply(pd.to_numeric)

    ################ Load data to dw_injuries

    # Data warehouse credentials for loading
    pg_hook_2 = PostgresHook(
        postgres_conn_id='dw_injuries',
        schema='dw_injuries'
    )
    # Connect to data warehouse for loading
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statements
    sql_create_table = "CREATE TABLE IF NOT EXISTS store_historical_injuries (" \
                       "code int , season VARCHAR(255), injury VARCHAR(255), date_from_day int," \
                       "date_from_mon int, date_from_year int, date_from date," \
                       "date_until_day int, date_until_mon int, date_until_year int," \
                       "date_until date, days_injured int, games_missed int, injury_id SERIAL NOT NULL PRIMARY KEY);"
    sql_truncate_table = "TRUNCATE TABLE store_historical_injuries"

    # Execute SQL statement
    cursor_2.execute(sql_create_table)
    cursor_2.execute(sql_truncate_table)
    pg_conn_2.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    pg_hook_2.insert_rows(table="store_historical_injuries", rows=rows)

# 5. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,EPL PLAYER INJURIES & BIOS LOADED TO DW')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

# Schedule for 04:30 daily
dag = DAG('5_dw_injuries_DAG',
          schedule_interval = '30 04 * * *',
          catchup = False,
          default_args = default_args)

# ----------------------------- Set Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_DAG,
    dag = dag
)

# 2. Assign ID's
assign_ids_task = PythonOperator(
    task_id = "assign_ids_task",
    python_callable = assign_ids,
    dag = dag
)

# 3. Bios Table
bios_task = PythonOperator(
    task_id = "bios_task",
    python_callable = bios,
    dag = dag
)

# 4. Historical Injuries Table
create_injuries_task = PythonOperator(
    task_id = "create_injuries_task",
    python_callable = injuries,
    dag = dag
)

# 5. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> assign_ids_task >> bios_task >> create_injuries_task >> end_task

