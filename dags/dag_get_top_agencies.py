from airflow.decorators import dag, task #Airflow TaskFlow
from datetime import datetime
from src.pipelines.get_top_agencies import (
    get_top_agencies_from_api,
    get_top_agencies
)
from src.pipelines.load_data_into_postgres import load_data_into_database
import logging
logger = logging.getLogger(__name__)

@dag(
    dag_id="dag_top_agencies",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",  # Run daily at midnight
    catchup=False,
    tags=["api", "top_agencies"],
)
def dag_top_agencies():
    #Retries handled within function
    @task()
    def extract_task():
        logger.info("Executing get_top_agencies task...")
        return get_top_agencies()

    @task
    def load_task(df):
        logger.info("Executing load_data_into_database task for top_agencies...")
        return load_data_into_database(df = df, table_name = 'top_agencies')

    df = extract_task()
    load_task(df)

dag_top_agencies()