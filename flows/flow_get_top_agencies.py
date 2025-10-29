from prefect import flow, task
from scripts.api_to_db_scripts.get_top_agencies import (
    get_top_agencies_from_api,
    transform_top_agencies
)
from scripts.api_to_db_scripts.load_data_into_postgres import load_data_into_database


# Retries handled in function code
@task
def get_data():
    return transform_top_agencies()

# Retries handled in function code
@task
def load_data(df):
    return load_data_into_database(df = df, table_name = 'top_agencies')


@flow(name="API to Postgres ETL Pipeline for Top Agency data")
def flow_top_agencies():
    transformed_data = get_data()
    load_data(transformed_data)

if __name__ == "__main__":
    flow_top_agencies()
