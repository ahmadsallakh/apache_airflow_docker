from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "ahmad", "retries": 5, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="dag_with_catchup",
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    task1 = BashOperator(task_id="task1", bash_command="echo This is a simple bash command!")
