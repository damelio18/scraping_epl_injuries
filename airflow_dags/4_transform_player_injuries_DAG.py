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
    # Data lake credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='datalake1_airflow',
        schema='datalake1'
    )
    # Connect to data lake
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT player, dob, height, nationality, int_caps," \
                             "int_goals, current_club, season, injury, date_from," \
                             "date_until, days, games_missed FROM historical_injuries;"

    # Fetch data
    cursor_1.execute(sql_statement_get_data)
    tuples_list = cursor_1.fetchall()

    # Data warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statements
    sql_statement_drop = "DROP TABLE IF EXISTS stg_historical_injuries"
    sql_statement_create_table = "CREATE TABLE IF NOT EXISTS stg_historical_injuries (player VARCHAR(255)," \
                                 "dob VARCHAR(255), height VARCHAR(255), nationality VARCHAR(255), " \
                                 "int_caps VARCHAR(255), int_goals VARCHAR(255), current_club VARCHAR(255)," \
                                 "season VARCHAR(255), injury VARCHAR(255),date_from VARCHAR(255), " \
                                 "date_until VARCHAR(255), days VARCHAR(255), games_missed VARCHAR(255));"

    # Execute SQL statements
    cursor_2.execute(sql_statement_drop)
    cursor_2.execute(sql_statement_create_table)
    for row in tuples_list:
        cursor_2.execute('INSERT INTO stg_historical_injuries VALUES %s', (row,))
    pg_conn_2.commit()


# 3. Missing Values
def missing_values():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch data
    tuples_list = dw_cursor.fetchall()

    # Create DataFrame
    column_names = ['player', 'dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Replace the empty strings and '-'
    df = df.replace(['NA'], np.nan)
    df['date_until'] = df['date_until'].replace(['-'], np.nan)
    df['games_missed'] = df['games_missed'].replace(['?', '-'], "0").astype('float')
    df[['int_caps', 'int_goals']] = df[['int_caps', 'int_goals']].fillna('0')

    # SQL Statement
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Execute SQL statement
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
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch data
    tuples_list = dw_cursor.fetchall()

    # Create DataFrame
    column_names = ['player', 'dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Strip all leading and trailing whitespace from player names
    df['player'] = df.player.str.strip()

    # Split name into first and second name
    df[['first_name', 'second_name']] = df['player'].str.split(' ', n=1, expand=True)

    # Remove players with no first name
    df = df[~df['first_name'].isnull()]

    # Drop player column
    df = df.drop(['player'], axis=1)

    # SQL Statements
    sql_alter = "ALTER TABLE stg_historical_injuries ADD first_name VARCHAR(255)," \
                  "ADD second_name VARCHAR(255), DROP COLUMN player;"
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Execute SQL statements
    dw_cursor.execute(sql_alter)
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)


# 5. Clean dates
def date_columns():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch all data from table
    tuples_list = dw_cursor.fetchall()

    # Create DataFrame
    column_names = ['dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed',
                    'first_name', 'second_name']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Standarise format of all date columns to ('mmm dd, yyy')
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

    # Replace NaT type with np.nan
    df = df.where(pd.notnull(df), None)

    # SQL Statement
    sql_alter = "ALTER TABLE stg_historical_injuries ADD dob_day VARCHAR(255)," \
                "ADD dob_mon VARCHAR(255), ADD dob_year VARCHAR(255), ADD age VARCHAR(255)," \
                "ADD date_from_day VARCHAR(255), ADD date_from_mon VARCHAR(255)," \
                "ADD date_from_year VARCHAR(255), ADD date_until_day VARCHAR(255)," \
                "ADD date_until_mon VARCHAR(255), ADD date_until_year VARCHAR(255)"
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Execute SQL statements
    dw_cursor.execute(sql_alter)
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)

# 6. Current clubs
def current_club():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch data
    tuples_list = dw_cursor.fetchall()

    # Create DataFrame
    column_names = ['dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed',
                    'first_name', 'second_name', 'dob_day', 'dob_mon',
                    'dob_year', 'age', 'date_from_day', 'date_from_mon',
                    'date_from_year', 'date_until_day', 'date_until_mon',
                    'date_until_year']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Create clubs dictionary
    club_dict = {'Arsenal FC': 'Arsenal', 'Aston Villa': 'Aston Villa',
                 'AFC Bournemouth': 'Bournemouth', 'Brentford FC': 'Brentford',
                 'Brighton & Hove Albion U21': 'Brighton', 'Brighton & Hove Albion': 'Brighton',
                 'Chelsea FC': 'Chelsea', 'Crystal Palace': 'Crystal Palace',
                 'Everton FC': 'Everton', 'Fulham FC': 'Fulham',
                 'Leicester City': 'Leicester', 'Leeds United U21': 'Leeds',
                 'Leeds United': 'Leeds', 'Liverpool FC': 'Liverpool',
                 'Manchester City': 'Man City', 'Manchester United': 'Man Utd',
                 'Newcastle United': 'Newcastle', 'Nottingham Forest': 'Nott\'m Forest',
                 'Southampton FC': 'Southampton', 'Tottenham Hotspur': 'Spurs',
                 'West Ham United': 'West Ham', 'Wolverhampton Wanderers': 'Wolves'}

    # Change names of clubs
    df['current_club'] = df['current_club'].map(club_dict)

    # SQL Statement
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Execute SQL statements
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)


# 7. Season
def seasons():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch data
    tuples_list = dw_cursor.fetchall()

    # Create DataFrame
    column_names = ['dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed',
                    'first_name', 'second_name', 'dob_day', 'dob_mon',
                    'dob_year', 'age', 'date_from_day', 'date_from_mon',
                    'date_from_year', 'date_until_day', 'date_until_mon',
                    'date_until_year']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Create seasons dictionary
    season_dict = {'22/23': '2022/23', '21/22': '2021/22', '20/21': '2020/21',
                   '19/20': '2019/20', '18/19': '2018/19', '17/18': '2017/18',
                   '16/17': '2016/17', '15/16': '2015/16', '14/15': '2014/15',
                   '13/14': '2013/14', '12/13': '2012/13', '11/12': '2011/12',
                   '10/11': '2010/11', '09/10': '2009/10', '08/09': '2008/09',
                   '07/08': '2007/08', '06/07': '2006/07', '05/06': '2005/06',
                   '04/05': '2004/05', '03/04': '2003/04', '02/03': '2002/03'}

    # Change names of seasons
    df['season'] = df['season'].map(season_dict)

    # SQL Statement
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Execute SQL statements
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)


# 8. Days injured
def days_injured():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statement
    sql_statement = "SELECT * FROM stg_historical_injuries;"

    # Execute SQL statements
    dw_cursor.execute(sql_statement)

    # Fetch data
    tuples_list = dw_cursor.fetchall()

    # Create DataFrame
    column_names = ['dob', 'height', 'nationality', 'int_caps',
                    'int_goals', 'current_club', 'season', 'injury',
                    'date_from', 'date_until','days', 'games_missed',
                    'first_name', 'second_name', 'dob_day', 'dob_mon',
                    'dob_year', 'age', 'date_from_day', 'date_from_mon',
                    'date_from_year', 'date_until_day', 'date_until_mon',
                    'date_until_year']

    df = pd.DataFrame(tuples_list, columns = column_names)

    # Clean days injured
    df['days'] = df['days'].str.rstrip(' days').astype('float')

    # SQL Statement
    sql_alter = "ALTER TABLE stg_historical_injuries RENAME COLUMN days TO days_injured"
    sql_truncate_table = "TRUNCATE TABLE stg_historical_injuries"

    # Execute SQL statements
    dw_cursor.execute(sql_alter)
    dw_cursor.execute(sql_truncate_table)
    dw_pg_conn.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in df.values]

    # Insert the rows into the database
    dw_pg_hook.insert_rows(table="stg_historical_injuries", rows=rows)


# 9. Store table
def store_table():
    # Data warehouse credentials
    dw_pg_hook = PostgresHook(
        postgres_conn_id='injuries',
        schema='injuries'
    )
    # Connect to data warehouse
    dw_pg_conn = dw_pg_hook.get_conn()
    dw_cursor = dw_pg_conn.cursor()

    # SQL Statements
    sql_drop_store = "DROP TABLE IF EXISTS store_clean_historical_injuries;"
    sql_store_table = "CREATE TABLE store_clean_historical_injuries AS TABLE stg_historical_injuries;"
    sql_drop_staging = "DROP TABLE stg_historical_injuries"

    # Execute SQL statements
    dw_cursor.execute(sql_drop_store)
    dw_cursor.execute(sql_store_table)
    dw_cursor.execute(sql_drop_staging)
    dw_pg_conn.commit()


# 10. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED,TRANSFORMED EPL PLAYER INJURIES')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

# Schedule for 03:30 daily
dag = DAG('4_transform_player_injuries_DAG',
          schedule_interval = '30 03 * * *',
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

# 6. Current Club
current_club_task = PythonOperator(
    task_id = "current_club_task",
    python_callable = current_club,
    dag = dag
)

# 7. Season
season_task = PythonOperator(
    task_id = "season_task",
    python_callable = seasons,
    dag = dag
)

# 8. Days Injured
days_injured_task = PythonOperator(
    task_id = "days_injured_task",
    python_callable = days_injured,
    dag = dag
)

# 9. Store Table
store_table_task = PythonOperator(
    task_id = "store_table_task",
    python_callable = store_table,
    dag = dag
)

# 10. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------

start_task >> stg_table_task >> missing_values_task >> player_names_task >> clean_dates_task >> current_club_task >> season_task >> days_injured_task >> store_table_task >> end_task