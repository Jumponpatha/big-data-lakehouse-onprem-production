from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="initial_spark_create_lakehouse_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Run SparkSubmitOperator
    Initial_create_lakehouse = SparkSubmitOperator(
        task_id="run_initial_create_database_task",
        application="/opt/airflow/src/init/init_databases.py",
        conn_id="spark_default",
    )

    Initial_create_table = SparkSubmitOperator(
        task_id="run_initial_create_tables_task",
        application="/opt/airflow/src/init/init_tables.py",
        conn_id="spark_default",
    )