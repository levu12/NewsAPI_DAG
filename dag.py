from datetime import datetime, timedelta
import importlib 
from airflow.operators.python import PythonOperator
from airflow import DAG

DAG_DESC = "DAG for the NewsAPI data pipeline"
DAG_ID = "newsapi_data_pipeline"
OWNER = "levu"
START_DATE = datetime(2024, 6, 18)
SCHEDULE = "0 0 * * *"

default_args = {
    "owner": OWNER,
    "start_date": START_DATE,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 10
}

def start(**kwargs) -> None:
    print("DAG started")

def extract(**kwargs) -> None:
    print("Extracting data from NewsAPI")
    news_extract = importlib.import_module('newsetl')
    news_extract.main()

with DAG(
    DAG_ID,
    default_args=default_args,
    description=DAG_DESC,
    schedule_interval=SCHEDULE,
    catchup=False,
    tags=["newsapi", "data-pipeline"],
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=start,
        provide_context=True
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        provide_context=True
    )

    start >> extract