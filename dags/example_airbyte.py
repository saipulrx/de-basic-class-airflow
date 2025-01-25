from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

@dag(
    start_date=datetime(2025,1,7),
    schedule='@daily',
    catchup=False,
    tags=['airbyte','airflow'],
)

def ingest_data_airbyte():
    postgres_to_bigquery = AirbyteTriggerSyncOperator(
        task_id='ingest_postgres_to_bigquery',
        airbyte_conn_id='airbyte_con',
        connection_id='d9851725-604f-49c3-86b1-d69e4209a3dd',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    postgres_to_bigquery
    
ingest_data_airbyte()