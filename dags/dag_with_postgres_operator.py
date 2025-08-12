from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {"owner": "ahmad", "retries": 5, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="dag_with_postgres_operator_v01", default_args=default_args, start_date=datetime(2025, 8, 11), schedule_interval="0 0 * * *"
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id="create_postgres_table",
        conn_id="postgres_localhost",
        sql="""
        CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
        )
        """,
    )
