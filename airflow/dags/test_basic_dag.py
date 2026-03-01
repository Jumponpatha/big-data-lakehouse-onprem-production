from airflow import DAG
from datetime import datetime, timedelta
from airflow.sdk import task
with DAG(
    dag_id="test_basic_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    @task
    def print_hello():
        print("Airflow 3.1.6 is working 🚀")

    @task
    def add_numbers():
        result = 10 + 20
        print(f"Result: {result}")
        return result

    @task
    def print_result(value):
        print(f"Received: {value}")

    value = add_numbers()
    print_hello() >> value >> print_result(value)