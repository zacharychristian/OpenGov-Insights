from airflow.decorators import dag, task #Airflow TaskFlow
from datetime import datetime
from src.pipelines.get_award_by_state import (
    get_award_by_state_from_api,
    get_award_by_state
)
from src.pipelines.load_data_into_postgres import load_data_into_database
import logging
logger = logging.getLogger(__name__)

@dag(
    dag_id="dag_award_by_state",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",  # Run daily at midnight
    catchup=False,
    tags=["api", "award_by_state"],
)
def dag_award_by_state():
    #Retries handled within function
    @task()
    def extract_task():
        logger.info("Executing get_award_by_state task...")
        return get_award_by_state()

    @task
    def load_task(df):
        logger.info("Executing load_data_into_database task for awards_by_state...")
        return load_data_into_database(df = df, table_name = 'awards_by_state')

    df = extract_task()
    load_task(df)

dag_award_by_state()