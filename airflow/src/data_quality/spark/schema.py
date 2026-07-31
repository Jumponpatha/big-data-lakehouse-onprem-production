from src.config.logger import get_logger

logger = get_logger(__name__)

# Validate Column in Lakehouse
def validate_column(df, columns):
    '''Validate that the specified columns exist in the DataFrame.'''
    if isinstance(columns, str):
        columns = [columns]

    missing_cols = [col for col in columns if col not in df.columns]

    if missing_cols:
        logger.error(f"Missing columns: {missing_cols}")
        raise ValueError(f"Missing columns in DataFrame: {missing_cols}")

    logger.info(f"All columns validated successfully: {columns}")

# Check if table exists in Spark Catalog
def check_table_exists(spark, table_full_name: str) -> bool:
    '''Check if a table exists in the Spark catalog.'''
    try:
        return spark.catalog.tableExists(table_full_name)
    except Exception as e:
        logger.error(f"Error checking existence of table '{table_full_name}': {e}")
        return False