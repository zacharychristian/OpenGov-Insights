from airflow.decorators import dag, task #Airflow TaskFlow
from datetime import datetime
from src.pipelines.get_contract_counts_by_agency import (
    get_contract_counts_by_year_from_api,
    get_contract_counts_by_agency
)
from src.pipelines.load_data_into_postgres import load_data_into_database
import logging
logger = logging.getLogger(__name__)

@dag(
    dag_id="dag_contract_counts_by_agency",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",  # Run daily at midnight
    catchup=False,
    tags=["api", "contract_counts_by_agency"],
)
def dag_contract_counts_by_agency():
    #Retries handled within function
    @task()
    def extract_task():
        logger.info("Executing get_contract_counts_by_agency task...")
        return get_contract_counts_by_agency()

    @task
    def load_task(df):
        logger.info("Executing load_data_into_database task for contract_counts_by_agency...")
        return load_data_into_database(df = df, table_name = 'agency_contract_counts')

    df = extract_task()
    load_task(df)

dag_contract_counts_by_agency()