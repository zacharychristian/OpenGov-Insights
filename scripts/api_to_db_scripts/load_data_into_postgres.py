def load_data_into_database(table_name, df, max_retries=3, backoff=2):
    import time
    import psycopg2
    import psycopg2.extras as extras
    """
    Insert records into a PostgreSQL table with retry logic.

    Retries transient database errors (e.g., connection issues or locks)
    with exponential backoff between attempts.

    Args:
        conn: Active psycopg2 connection object.
        table_name (str): Target table name.
        cols (str): Comma-separated column names.
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
    conn = psycopg2.connect(dbname="usaSpending", user="postgres", password="Password123", host="localhost")

    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    attempt = 0

    while attempt < max_retries:
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
            print(f"Data successfully inserted into '{table_name}'.")
            return True

        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            attempt += 1
            print(f"Attempt {attempt} failed: {error}")

            if attempt < max_retries:
                sleep_time = backoff ** attempt  # exponential backoff
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("Max retries reached. Insert failed.")
                return False

        finally:
            cursor.close()
