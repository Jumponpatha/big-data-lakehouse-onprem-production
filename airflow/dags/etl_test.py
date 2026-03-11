from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_lakehouse_job",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="run_spark_etl",
        application="/opt/airflow/src/etl_jobs.py",
        conn_id="spark_default",
    )