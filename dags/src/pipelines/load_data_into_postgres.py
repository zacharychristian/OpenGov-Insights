import logging
logger = logging.getLogger(__name__)

def load_data_into_database(table_name, df, max_retries=3, backoff=2):
    import time
    import psycopg2
    import psycopg2.extras as extras

    """
    Insert records into a PostgreSQL table with retry logic.

    Retries transient database errors (e.g., connection issues or locks)
    with exponential backoff between attempts.

    Columns in dataframe must match relevant PostgreSQL database columns

    Only connects to usaSpending local PostgreSQL database.

    Args:
        table_name (str): Target table name.
        tuples (list[tuple]): Data to insert.
        max_retries (int, optional): Max number of retry attempts.
        backoff (int, optional): Base seconds for exponential backoff.

    Returns:
        bool: True if insert succeeded, False otherwise.
    
    Doesnt use SQLAlchemy because pandas and postgres in sqlAlchemy dont play well together 
    https://stackoverflow.com/questions/58664141/how-to-write-data-frame-to-postgres-table-without-using-sqlalchemy-engine
    https://stackoverflow.com/questions/12206600/how-to-speed-up-insertion-performance-in-postgresql
    """
    # Convert DataFrame to a list of tuples
    tuples = [tuple(x) for x in df.to_numpy()]
        
    # Get column names for the SQL query
    cols = ','.join(list(df.columns))
    #conn = psycopg2.connect(dbname="usaSpending", user="postgres", password="password", host="localhost") #Works on prefect
    conn = psycopg2.connect(dbname="usaSpending", user="postgres", password="password", host="host.docker.internal")

    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    attempt = 0

    while attempt < max_retries:
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
            #No data found or an error occurred.(f"Data successfully inserted into '{table_name}'.")
            logger.info(f"Data successfully inserted into '{table_name}'.")
            return True

        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            attempt += 1
            #No data found or an error occurred.(f"Attempt {attempt} failed: {error}")
            logger.info(f"Attempt {attempt} failed: {error}")

            if attempt < max_retries:
                sleep_time = backoff ** attempt  # exponential backoff
                #No data found or an error occurred.(f"Retrying in {sleep_time} seconds...")
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                #No data found or an error occurred.("Max retries reached. Insert failed.")
                logger.info("Max retries reached. Insert failed.")
                return False

        finally:
            cursor.close()
