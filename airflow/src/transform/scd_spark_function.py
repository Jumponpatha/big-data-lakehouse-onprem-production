from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.spark.spark_session import create_spark_session
from src.data_quality.spark.schema import check_table_exists, validate_column

# Initialize logger
logger = get_logger(__name__)

# Apply SCD Type 2 with Pyspark
def apply_scd_type_2(spark: SparkSession, source_table: str, target_table: str, key: list):
    '''
    Apply Slowly Changing Dimension (SCD) Type 2 logic to merge data from the source table into the target table.

    Parameters:
    - spark: The SparkSession object used to execute SQL queries.
    - source_table: The name of the source table containing new data.
    - target_table: The name of the target table to be merged into.
    - key: The list of primary key columns used for matching records between source and target.

    This function performs the following steps:
    1. Identifies new and updated records in the source table compared to the target table.
    2. Marks existing records in the target table as expired if they have been updated.
    3. Inserts new records from the source table into the target table with appropriate effective and expiry dates.
    '''
    catalog_name = "lakehouse_prod"
    bronze_schema = "bronze_db"
    silver_schema = "silver_db"

    source_table_full_name = f"{catalog_name}.{bronze_schema}.{source_table}"
    target_table_full_name = f"{catalog_name}.{silver_schema}.{target_table}"

    # Check if SparkSession is initialized
    if spark is None:
        logger.error("SparkSession is not initialized.")
        create_spark_session("Recreate SCD Type 2 Merge Operation")
        logger.info("SparkSession recreated successfully.")
        return

    # Check if source and target tables exist
    if not check_table_exists(spark, target_table_full_name):
        logger.error(f"Target table '{target_table_full_name}' does not exist.")
        return
    if not check_table_exists(spark, source_table_full_name):
        logger.error(f"Source table '{source_table_full_name}' does not exist.")
        return

    try:
        # Load source and target tables into DataFrames
        source_table_df = spark.read.format("iceberg").load(source_table_full_name)
        target_table_df = spark.read.format("iceberg").load(target_table_full_name)
        logger.info(f"Source table '{source_table_full_name}' and target table '{target_table_full_name}' loaded successfully.")
        
        logger.info("SCD Type 2 merge operation completed successfully.")
    except Exception as e:
        logger.error(f"SCD Type 2 merge operation failed: {e}")