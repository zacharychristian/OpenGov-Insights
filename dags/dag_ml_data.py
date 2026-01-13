from airflow.decorators import dag, task #Airflow TaskFlow
from datetime import datetime
from src.pipelines.get_ml_data import (
    get_ml_data_from_api,
    get_ml_data
)
from src.pipelines.load_data_into_postgres import load_data_into_database
import logging
logger = logging.getLogger(__name__)

@dag(
    dag_id="dag_ml_data",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",  # Run daily at midnight
    catchup=False,
    tags=["api", "ml_data"],
)
def dag_ml_data():
    #Retries handled within function
    @task()
    def extract_task():
        logger.info("Executing get_ml_data task...")
        return get_ml_data()

    @task
    def load_task(df):
        logger.info("Executing load_data_into_database task for ml_data...")
        return load_data_into_database(df = df, table_name = 'ml_data')

    df = extract_task()
    load_task(df)

dag_ml_data()