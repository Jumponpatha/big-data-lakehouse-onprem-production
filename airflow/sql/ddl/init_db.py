from src.spark.spark_session import create_spark_session

spark = create_spark_session("Create Initial Database")

spark.sql("""CREATE DATABASE IF NOT EXISTS lakehouse_prod.bronze_db""")
spark.sql("""CREATE DATABASE IF NOT EXISTS lakehouse_prod.silver_db""")
spark.sql("""CREATE DATABASE IF NOT EXISTS lakehouse_prod.gold_db""")