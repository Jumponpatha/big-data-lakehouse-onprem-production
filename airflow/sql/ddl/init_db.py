from utils.spark.spark_session import create_spark_session

spark = create_spark_session("Create Initial Database")

spark.sql("""
    CREATE DATABASE IF NOT EXISTS lakehouse_prod
    """)