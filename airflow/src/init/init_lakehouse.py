from src.spark.spark_session import create_spark_session
from src.config.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

try:
    spark = create_spark_session("Create Initial Database")

    logger.info("Starting database initialization ...")

    # Create databases for bronze, silver, and gold layers in the lakehouse
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS lakehouse_prod.bronze_db
            LOCATION 's3a://lakehouse-gold-bucket/warehouse/bronze'
    """)
    logger.info("Bronze database initialized.")

    spark.sql("""
            CREATE DATABASE IF NOT EXISTS lakehouse_prod.silver_db
            LOCATION 's3a://lakehouse-gold-bucket/warehouse/silver'
    """)
    logger.info("Silver database initialized.")

    spark.sql("""
            CREATE DATABASE IF NOT EXISTS lakehouse_prod.gold_db
            LOCATION 's3a://lakehouse-gold-bucket/warehouse/gold'
    """)
    logger.info("Gold database initialized.")

    logger.info("Finished database initialization completed.")
except Exception as e:
    logger.error(f"Job failed: {e}")

finally:
    if spark:
        spark.stop()