from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    start_date=datetime(2025,7,28),
    schedule='@daily',
    catchup=False,
    tags=['postgresql','airflow','covid','end_to_end'],
    default_args = default_args
)

def etl_data_covid():
    start_job = BashOperator(
        task_id = 'start_etl_job',
        bash_command = 'echo start ETL Pipeline covid data to PostgreSQl Using Airflow'
    )

    insert_all_covid_data = BashOperator(
        task_id = 'insert_all_covid_data',
        bash_command = 'python /opt/airflow/dags/script/etl_owid-covid-all.py'
    )

    daily_cases = BashOperator(
        task_id = 'daily_cases',
        bash_command = 'python /opt/airflow/dags/script/etl_owid_covid_daily_cases.py'
    )

    total_cases_by_continent = BashOperator(
        task_id = 'total_cases_by_continent',
        bash_command = 'python /opt/airflow/dags/script/etl_covid_total_cases_by_continent.py'
    )

    total_vaccinations_by_date = BashOperator(
        task_id = 'total_vaccinations_by_date',
        bash_command = 'python /opt/airflow/dags/script/etl_covid_total_vaccinations.py'
    )

    pdp_per_kapita = BashOperator(
        task_id = 'pdp_per_kapita',
        bash_command = 'python /opt/airflow/dags/script/etl_covid_pdp_perkapita.py'
    )

    top_10_death_case = BashOperator(
        task_id = 'top_10_death_case',
        bash_command = 'python /opt/airflow/dags/script/etl_covid_top_10_death_case.py'
    )

    end_job = BashOperator(
        task_id = 'end_etl_job',
        bash_command = 'echo end of ETL Pipeline covid data to PostgreSQl Using Airflow'
    )

    start_job >> [insert_all_covid_data, daily_cases, total_cases_by_continent]
    insert_all_covid_data >> [total_vaccinations_by_date, pdp_per_kapita, top_10_death_case]
    [total_vaccinations_by_date, pdp_per_kapita, top_10_death_case, daily_cases, total_cases_by_continent] >> end_job
    
etl_data_covid()