from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator

@dag(
    start_date=datetime(2025,1,7),
    schedule='@daily',
    catchup=False,
    tags=['airbyte','airflow'],
)

def ingest_data_airbyte_bigquery():
    start_job = BashOperator(
        task_id = 'start_job',
        bash_command = 'echo start EL Pipeline to BigQuery Using Airbyte and Airflow'
    )

    postgres_to_bigquery = AirbyteTriggerSyncOperator(
        task_id='ingest_postgres_to_bigquery',
        airbyte_conn_id='airbyte_con',
        connection_id='d9851725-604f-49c3-86b1-d69e4209a3dd',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    csv_to_bigquery = AirbyteTriggerSyncOperator(
        task_id='ingest_csv_to_bigquery',
        airbyte_conn_id='airbyte_con',
        connection_id='c899f24c-fd38-4490-8e82-1bbda1e583ae',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    end_job = BashOperator(
        task_id = 'end_job',
        bash_command = 'echo end of EL Pipeline to BigQuery Using Airbyte and Airflow'
    )

    start_job >> postgres_to_bigquery >> end_job
    start_job >> csv_to_bigquery >> end_job
    
ingest_data_airbyte_bigquery()