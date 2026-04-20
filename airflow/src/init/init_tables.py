from src.spark.spark_session import create_spark_session
from src.config.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

try:
    spark = create_spark_session("Create Initial Tables")

    logger.info("Starting Tables initialization ...")

    # Create table DIM_COMPANY
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse_prod.gold_db.dim_company (
            company_key BIGINT,
            company_symbol STRING,
            company_shortname STRING,
            company_fullname STRING,
            company_display STRING,
            industry_key INT,
            business_description STRING,
            exchange_key INT,
            location_key INT,
            previous_name STRING,
            name_change_date DATE,
            phone STRING,
            fax_number STRING,
            website STRING,
            ipo_date DATE,
            country_key INT,
            currency_key STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_current BOOLEAN,
            effective_date DATE,
            expiry_date DATE
        )
        USING iceberg
        PARTITIONED BY (days(effective_date));
    """
    )

    # Create table DIM_CURRENCY
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse_prod.silver_db.dim_currency (
            CURRENCY_KEY VARCHAR(10),
            CURRENCY_NAME VARCHAR(30),
            CURRENCY_ISO_CODE VARCHAR(10),
            CREATED_AT TIMESTAMP,
            UPDATED_AT TIMESTAMP,
            IS_CURRENT BOOLEAN,
            EFFECTIVE_DATE TIMESTAMP,
            EXPIRY_DATE TIMESTAMP
            )
        USING iceberg
    """)
    logger.info("Bronze database initialized.")

    logger.info("Finished tables initialization completed.")
except Exception as e:
    logger.error(f"Job failed: {e}")

finally:
    if spark:
        spark.stop()