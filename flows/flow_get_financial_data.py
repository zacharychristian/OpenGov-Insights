from prefect import flow, task
from scripts.api_to_db_scripts.get_financial_data import (
    transform_financial_data,
    get_financial_data_from_api
)
from scripts.api_to_db_scripts.load_data_into_postgres import load_data_into_database


# Retries handled in function code
@task
def get_data():
    return transform_financial_data()

# Retries handled in function code
@task
def load_data(df):
    return load_data_into_database(df = df, table_name = 'financial_data')


@flow(name="API to Postgres ETL Pipeline for Financial Data")
def flow_financial_data():
    transformed_data = get_data()
    load_data(transformed_data)

if __name__ == "__main__":
    flow_financial_data()
