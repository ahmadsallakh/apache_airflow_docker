from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "ahmad", "retries": 5, "retry_delay": timedelta(minutes=5)}


def get_name(ti) -> str:
    ti.xcom_push(key="first_name", value="Jerry")
    ti.xcom_push(key="last_name", value="Fridman")


def greet(age: int, ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    print(f"Hello world! My name is {first_name} {last_name}, and I'm {age} years old.")


with DAG(
    default_args=default_args,
    dag_id="our_dag_with_python_operator_v3",
    description="Our first dag using Python operator",
    start_date=datetime(2025, 8, 8),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(task_id="greet", python_callable=greet, op_kwargs={"name": "Tom", "age": 20})
    task2 = PythonOperator(task_id="get_name", python_callable=get_name)

    task2 >> task1
