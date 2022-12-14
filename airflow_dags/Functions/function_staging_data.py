from airflow.hooks.postgres_hook import PostgresHook

# Function to connect to DW and get data from staging table
def access_staging_table():
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

    return tuples_list