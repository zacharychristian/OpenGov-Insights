from airflow.decorators import dag, task #Airflow TaskFlow
from datetime import datetime
from src.pipelines.get_top_contracts import (
    get_top_contracts_from_api,
    transform_top_contracts
)
from src.pipelines.load_data_into_postgres import load_data_into_database
import logging
logger = logging.getLogger(__name__)

@dag(
    dag_id="dag_top_contracts",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",  # Run daily at midnight
    catchup=False,
    tags=["api", "top_contracts"],
)

# The number of contracts from API can be customized. Default is 100
def dag_top_contracts(number_of_contracts = 100):
    #Retries handled within function
    @task()
    def extract_task():
        logger.info("Executing get_top_contracts_from_api task...")
        return get_top_contracts_from_api()

    @task()
    def transform_task(raw_data, num_contracts):
        logger.info("Executing transform_top_contracts task...")
        return transform_top_contracts(response = raw_data, numContracts = num_contracts)        

    @task
    def load_task(df):
        logger.info("Executing load_data_into_database task for top_contracts...")
        return load_data_into_database(df = df, table_name = 'top_contracts')

    df = extract_task()
    transformed_data = transform_task(raw_data = df, num_contracts = number_of_contracts)
    load_task(transformed_data)

dag_top_contracts()