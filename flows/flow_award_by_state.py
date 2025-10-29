from prefect import flow, task
from scripts.api_to_db_scripts.get_award_by_state import (
    get_award_by_state_from_api,
    get_award_by_state
)
from scripts.api_to_db_scripts.load_data_into_postgres import load_data_into_database


# Retries handled in function code
@task
def get_data():
    return get_award_by_state()

#Retries handled in function code
@task
def load_data(df):
    return load_data_into_database(df = df, table_name = 'awards_by_state')


@flow(name="API to Postgres ETL Pipeline for Awards by State")
def flow_award_by_state():
    award_by_state_file = get_data()
    load_data(award_by_state_file)

if __name__ == "__main__":
    flow_award_by_state()
