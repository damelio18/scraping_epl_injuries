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

    # Data warehouse credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='dw_injuries',
        schema='dw_injuries'
    )
    # Connect to data warehouse
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT * FROM store_player_bios;"

    # Execute SQL Statement
    cursor_1.execute(sql_statement_get_data)

    # Fetch data
    tuples_list_1 = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['code', 'first_name', 'second_name', 'current_club',
                    'dob_day', 'dob_mon', 'dob_year', 'dob', 'age', 'height',
                    'nationality', 'int_caps', 'int_goals', 'injury_risk']

    df1 = pd.DataFrame(tuples_list_1, columns = column_names)

    ################ Get fpl data from DW

    # Data warehouse credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='fantasypl',
        schema='fantasypl'
    )
    # Connect to data warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT * FROM store_gameweeks;"

    # Execute SQL Statement
    cursor_2.execute(sql_statement_get_data)

    # Fetch data
    tuples_list_2 = cursor_2.fetchall()

    # Create DataFrame
    df_cols = []
    column_names = [desc[0] for desc in cursor_2.description]
    for i in column_names:
        df_cols.append(i)

    df2 = pd.DataFrame(tuples_list_2, columns=df_cols)

    ################ Get current values data from DL

    # Data lake credentials
    pg_hook_3 = PostgresHook(
        postgres_conn_id='dl_fpl',
        schema='fpl_api'
    )
    # Connect to data lake
    pg_conn_3 = pg_hook_3.get_conn()
    cursor_3 = pg_conn_3.cursor()

    # SQL Statement
    sql_statement_get_data = "SELECT code, now_cost FROM elements;"

    # Execute SQL Statement
    cursor_3.execute(sql_statement_get_data)

    # Fetch data
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
    rows = df.values.tolist()

    return df_cols, rows


# 3. Create dim_tables
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

    #SQL Statement:
    sql_alter_table = "ALTER TABLE IF EXISTS fct_performance DROP CONSTRAINT IF EXISTS date_id_fk," \
                      "DROP CONSTRAINT IF EXISTS player_id_fk," \
                      "DROP CONSTRAINT IF EXISTS team_id_fk," \
                      "DROP CONSTRAINT IF EXISTS fixture_id_fk;"

    # Alter table
    cursor_1.execute(sql_alter_table)
    pg_conn_1.commit()

    # ################ Get predicted points from data warehouse
    #
    # # Data warehouse credentials
    # pg_hook_2 = PostgresHook(
    #     postgres_conn_id='fantasypl',
    #     schema='fantasypl'
    # )
    # # Connect to data warehouse
    # pg_conn_2 = pg_hook_2.get_conn()
    # cursor_2 = pg_conn_2.cursor()
    #
    # # SQL Statement
    # sql_statement_get_data = "SELECT * FROM stage_predictions;"
    #
    # # Execute SQL statement
    # cursor_2.execute(sql_statement_get_data)
    #
    # # Fetch data
    # tuples_list_2 = cursor_2.fetchall()
    #
    # # Create DataFrame
    # column_names = ['player_id', 'predicted_points']
    #
    # df2 = pd.DataFrame(tuples_list_2, columns=column_names)
    #
    # # Change type
    # df2['player_id'] = df['player_id'].astype(str)

    ################ dim_players

    # Select columns
    players = df[['player_id', 'name', 'age', 'height', 'nationality',
                  'int_caps', 'int_goals', 'injury_risk', 'position', 'now_cost']]

    # Drop unwanted columns
    df.drop(['name', 'age', 'height', 'nationality','int_caps', 'int_goals',
             'injury_risk', 'position', 'now_cost'], axis=1, inplace=True)

    # Change name of column
    players = players.rename(columns={"now_cost": "current_value"})

    # Drop duplicates
    players = players.drop_duplicates()

    # Change unit for value
    players['current_value'] = players['current_value'].astype(int)
    players['current_value'] = players['current_value'] / 10

    # Change type
    # players['player_id'] = players['player_id'].astype(str)

    # # Merge predicted points to dim_players
    # players2 = pd.merge(players, df2,
    #                     on=['player_id'], how='left')

    # # Drop duplicates
    # players2 = players2.drop_duplicates()

    # SQL Statements
    sql_create_table = "CREATE TABLE IF NOT EXISTS dim_players (player_id int PRIMARY KEY, " \
                       "player_name VARCHAR(255), age float, height float, nationality VARCHAR(255)," \
                       "int_caps float, int_goals float, injury_risk float, player_position VARCHAR(255)," \
                       "current_value float);"
    sql_truncate_table = "TRUNCATE TABLE dim_players;"

    # Drop and create table
    cursor_1.execute(sql_create_table)
    cursor_1.execute(sql_truncate_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in players.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="dim_players", rows=rows)

    ################ dim_teams.

    # Select columns
    team = df[['team_id', 'team']]

    # Drop unwanted columns
    df.drop('team', axis=1, inplace=True)

    # Drop duplicates
    team = team.drop_duplicates()

    # SQL Statements
    sql_create_table = "CREATE TABLE IF NOT EXISTS dim_teams (team_id int PRIMARY KEY, team VARCHAR(255));"
    sql_truncate_table = "TRUNCATE TABLE dim_teams;"

    # Drop and create table
    cursor_1.execute(sql_create_table)
    cursor_1.execute(sql_truncate_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in team.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="dim_teams", rows=rows)

    ################ dim_fixtures

    # Select columns
    fixture = df[['fixture_id', 'home_team', 'away_team', 'team_h_score', 'team_a_score','round']]

    # Drop unwanted columns
    df.drop(['home_team', 'away_team', 'team_h_score', 'team_a_score',
              'round'], axis=1, inplace=True)

    # # Drop duplicates
    fixture = fixture.drop_duplicates(subset=['fixture_id'])

    # SQL Statements
    sql_create_table = "CREATE TABLE IF NOT EXISTS dim_fixtures (fixture_id int PRIMARY KEY," \
                       "home_team VARCHAR(255), away_team VARCHAR(255), team_h_score int," \
                       "team_a_score int ,round int);"
    sql_truncate_table = "TRUNCATE TABLE dim_fixtures;"

    # Drop and create table
    cursor_1.execute(sql_create_table)
    cursor_1.execute(sql_truncate_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in fixture.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="dim_fixtures", rows=rows)

    ################ Push through xcoms

    # Column_names
    df_cols = df.columns.tolist()

    # Create a list of tuples representing the rows in the dataframe
    rows = df.values.tolist()

    return df_cols, rows

# 4. Create fct_table
def create_fct(ti):
    # Get data
    data = ti.xcom_pull(task_ids = ['create_dims_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Separate team name and team url
    df_cols = data[0][0]
    df_data = data[0][1]

    # Create Data Frame
    df = pd.DataFrame(df_data, columns=df_cols)

    ################ Connect to dw_performance

    # Data warehouse credentials
    pg_hook_1 = PostgresHook(
        postgres_conn_id='dw_performance',
        schema='dw_performance'
    )
    # Connect to data warehouse
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    ################ Create fct_table

    performance = df[['performance_id', 'date_id', 'player_id', 'team_id', 'fixture_id', 'total_points',
                       'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded', 'own_goals',
                       'penalties_saved', 'penalties_missed', 'yellow_cards', 'red_cards', 'saves', 'bonus',
                       'bps', 'influence', 'creativity', 'threat', 'ict_index', 'starts', 'expected_goals',
                       'expected_assists', 'expected_goal_involvements', 'expected_goals_conceded', 'value',
                       'transfers_balance', 'selected', 'transfers_in', 'transfers_out']]

    # Change unit for value
    performance['value'] = performance['value'].astype(int)
    performance['value'] = performance['value'] / 10

    # Change name of column
    performance = performance.rename(columns={"value": "value_at_time"})

    # SQL Statements
    sql_create_table = "CREATE TABLE IF NOT EXISTS fct_performance (performance_id int PRIMARY KEY," \
                       "date_id date, player_id int, team_id int, fixture_id int , total_points int," \
                       "minutes int, goals_scored int, assists int, clean_sheets int, goals_conceded int," \
                       "own_goals int, penalties_saved int, penalties_missed int, yellow_cards int," \
                       "red_cards int, saves int, bonus int, bps int, influence float, creativity float," \
                       "threat float, ict_index float, starts int, expected_goals float, expected_assists float," \
                       "expected_goal_involvements float, expected_goals_conceded float, value_at_time float," \
                       "transfers_balance int, selected float, transfers_in int, transfers_out int);"
    sql_truncate_table = "TRUNCATE TABLE fct_performance;"
    sql_alter_table = "ALTER TABLE fct_performance ADD CONSTRAINT date_id_fk FOREIGN KEY (date_id) REFERENCES dim_date (date_actual)," \
                      "ADD CONSTRAINT player_id_fk FOREIGN KEY (player_id) REFERENCES dim_players (player_id)," \
                      "ADD CONSTRAINT team_id_fk FOREIGN KEY (team_id) REFERENCES dim_teams (team_id)," \
                      "ADD CONSTRAINT fixture_id_fk FOREIGN KEY (fixture_id) REFERENCES dim_fixtures (fixture_id);"

    # Execute SQL Statements
    cursor_1.execute(sql_create_table)
    cursor_1.execute(sql_truncate_table)
    cursor_1.execute(sql_alter_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in performance.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="fct_performance", rows=rows)


# 5. Log the end of the DAG
def finish_DAG():
    logging.info('DAG HAS FINISHED, LOADED TO DW')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'Danny',
    'start_date': datetime.datetime(2022,12,2)
}

# Schedule for 05:00 daily
dag = DAG('6_dw_performance_DAG',
          schedule_interval = '0 05 * * *',
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

# 3. Create dim_tables
create_dims_task = PythonOperator(
    task_id = "create_dims_task",
    python_callable = create_dims,
    do_xcom_push=True,
    dag = dag
)

# 4. Create fct_table
create_fct_task = PythonOperator(
    task_id = "create_fct_task",
    python_callable = create_fct,
    dag = dag
)

# 5. End Task
end_task = PythonOperator(
    task_id = "end_task",
    python_callable = finish_DAG,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> join_data_task >> create_dims_task >> create_fct_task >> end_task