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

    ################ Get bios data from DW

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

    ################ Get fpl data from DW

    # Data warehouse: fpl
    pg_hook_2 = PostgresHook(
        postgres_conn_id='fantasypl',
        schema='fantasypl'
    )
    # Connect to data warehouse: fpl
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement: Get data
    sql_statement_get_data = "SELECT * FROM store_gameweeks;"

    # Fetch data
    cursor_2.execute(sql_statement_get_data)
    tuples_list_2 = cursor_2.fetchall()

    # Column_names
    df_cols = []
    column_names = [desc[0] for desc in cursor_2.description]
    for i in column_names:
        df_cols.append(i)

    df2 = pd.DataFrame(tuples_list_2, columns=df_cols)

    ################ Get current values data from DL

    # Data lake: fpl
    pg_hook_3 = PostgresHook(
        postgres_conn_id='dl_fpl',
        schema='fpl_api'
    )
    # Connect to data lake: fpl
    pg_conn_3 = pg_hook_3.get_conn()
    cursor_3 = pg_conn_3.cursor()

    # SQL Statement: Get data
    sql_statement_get_data = "SELECT code, now_cost FROM elements;"

    # Fetch data
    cursor_3.execute(sql_statement_get_data)
    tuples_list_3 = cursor_3.fetchall()

    # Create DataFrame
    column_names = ['code', 'now_cost']

    df3 = pd.DataFrame(tuples_list_3, columns=column_names)

    ################ Merge

    # Merge 1
    df = pd.merge(df2, df1[['code', 'age', 'height', 'nationality', 'int_caps', 'int_goals', 'injury_risk']],
                  on=['code'], how='left')

    # Merge 2
    df = pd.merge(df, df3[['code', 'now_cost']], on=['code'], how='left')

    # Create performance_id
    df.index = np.arange(1, len(df) + 1)
    df.reset_index(inplace=True)
    df = df.rename(columns={"index": "performance_id", "date": "date_id"})

    ################ Transform df

    # Players
    # Change name of column
    df = df.rename(columns={"id": "player_id"})

    # Create new column
    df.insert(2, "name", "")

    # Get full name
    df['name'] = df['first_name'].str.cat(df['second_name'], sep=" ")

    # Drop unwanted columns
    df.drop(['first_name', 'second_name', 'web_name', 'code'], axis=1, inplace=True)

    # Team
    # Dictionary Value
    key = df['team'].unique().tolist()

    # Dictionary Key
    value = []
    counter = 1
    for i in key:
        value.append(str(counter))
        counter = counter + 1

    # Create Dictionary
    res = dict(zip(key, value))

    # Create new column
    df.insert(10, "team_id", "")

    # Change values in column
    df['team_id'] = df['team'].map(res)

    # Fixtures
    # Dictionary Value
    key = df['fixture'].unique().tolist()

    # Dictionary Key
    value = []
    counter = 1
    for i in key:
        value.append(str(counter))
        counter = counter + 1

    # Create Dictionary
    res = dict(zip(key, value))

    # Create new column
    df.insert(12, "fixture_id", "")

    # Change values in column
    df['fixture_id'] = df['fixture'].map(res)

    # Create new column
    df.insert(14, "home_team", "")

    # Create new column
    df.insert(15, "away_team", "")

    # Create home and away column
    df[['home_team', 'away_team']] = df.fixture.str.split("-", expand=True)

    # Drop unwanted columns
    df.drop(['fixture', 'opponent_team', 'was_home', 'kickoff_time'], axis=1, inplace=True)

    ################ Push through xcoms
    df = df.astype(str)

    # # Column_names
    df_cols = df.columns.tolist()

    # Create a list of tuples representing the rows in the dataframe
    #rows = [tuple(x) for x in df.values]
    rows = df.values.tolist()

    return df_cols, rows


# 3. Create star schema
def create_dims(ti):
    # get data returned from 'scrape_team_urls_task'
    data = ti.xcom_pull(task_ids = ['join_data_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Separate team name and team url
    df_cols = data[0][0]
    df_data = data[0][1]

    # Create Data Frame
    df = pd.DataFrame(df_data, columns=df_cols)

    ################ Connect to dw_performance

    # Data warehouse: injuries
    pg_hook_1 = PostgresHook(
        postgres_conn_id='dw_performance',
        schema='dw_performance'
    )
    # Connect to data warehouse: injuries
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    ################ dim_players

    # Select columns
    players = df[['player_id', 'name', 'age', 'height', 'nationality',
                  'int_caps', 'int_goals', 'injury_risk', 'position', 'now_cost']]

    # Drop unwanted columns
    df.drop(['name', 'age', 'height', 'nationality','int_caps', 'int_goals',
             'injury_risk', 'position', 'now_cost'], axis=1, inplace=True)

    # Drop duplicates
    players = players.drop_duplicates()

    # SQL Statement: Create new table
    sql_create_table = "CREATE TABLE IF NOT EXISTS dim_players (player_id int PRIMARY KEY, " \
                       "player_name VARCHAR(255), age float, height float, nationality VARCHAR(255)," \
                       "int_caps float, int_goals float, injury_risk float, player_position VARCHAR(255)," \
                       "now_cost float);"

    sql_truncate_table = "TRUNCATE TABLE dim_players;"

    # Drop and create table
    cursor_1.execute(sql_create_table)
    cursor_1.execute(sql_truncate_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in players.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="dim_players", rows=rows)

    ################ dim_teams

    # Select columns
    team = df[['team_id', 'team']]

    # Drop unwanted columns
    df.drop('team', axis=1, inplace=True)

    # Drop duplicates
    team = team.drop_duplicates()

    # SQL Statement: Create new table
    sql_create_table = "CREATE TABLE IF NOT EXISTS dim_teams (team_id int PRIMARY KEY, team VARCHAR(255)););"

    sql_truncate_table = "TRUNCATE TABLE dim_teams;"

    # Drop and create table
    cursor_1.execute(sql_create_table)
    cursor_1.execute(sql_truncate_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in team.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="dim_teams", rows=rows)







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
    do_xcom_push = True,
    dag = dag
)

# 3. Create dim_players
create_dims_task = PythonOperator(
    task_id = "create_dims_task",
    python_callable = create_dims,
    dag = dag
)




# .... End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> join_data_task >> create_dims_task >> end_task