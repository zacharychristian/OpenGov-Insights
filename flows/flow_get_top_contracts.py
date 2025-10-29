from prefect import flow, task
from scripts.api_to_db_scripts.get_top_contracts import (
    get_top_contracts_from_api,
    transform_top_contracts
)
from scripts.api_to_db_scripts.load_data_into_postgres import load_data_into_database


@task 
def get_raw_data(num_contracts):
    return get_top_contracts_from_api(numContracts = num_contracts)

# Retries handled in function code
@task
def get_data(data, num_contracts):
    return transform_top_contracts(data, numContracts = num_contracts)

# Retries handled in function code
@task
def load_data(df):
    return load_data_into_database(df = df, table_name = 'top_contracts')


@flow(name="API to Postgres ETL Pipeline for Top Contract data")
def flow_top_contracts(number_of_contracts = 100):
    raw_data = get_raw_data(num_contracts = number_of_contracts)
    transformed_data = get_data(data = raw_data, num_contracts = number_of_contracts)
    load_data(transformed_data)

if __name__ == "__main__":
    flow_top_contracts()
