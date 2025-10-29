from prefect import flow, task
from scripts.api_to_db_scripts.get_contract_counts_by_agency import (
    get_contract_counts_by_year_from_api, #Import function used by get_contract_counts_by_agency
    get_contract_counts_by_agency
)
from scripts.api_to_db_scripts.load_data_into_postgres import load_data_into_database


# Retries handled in function code
@task
def get_data():
    return get_contract_counts_by_agency()

#Retries handled in function code
@task
def load_data(df):
    return load_data_into_database(df = df, table_name = 'agency_contract_counts')


@flow(name="API to Postgres ETL Pipeline for Contract Counts by Agency")
def flow_contract_counts_by_agency():
    transformed_data = get_data()
    load_data(transformed_data)

if __name__ == "__main__":
    flow_contract_counts_by_agency()
