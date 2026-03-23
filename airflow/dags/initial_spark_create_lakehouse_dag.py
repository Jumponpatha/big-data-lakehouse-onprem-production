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
    spark_job = SparkSubmitOperator(
        task_id="run_initial_create_lakehouse_task",
        application="/opt/airflow/src/init/init_lakehouse.py",
        conn_id="spark_default",
    )