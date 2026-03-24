from pyspark.sql.functions import col
from src.config.logger import get_logger
from src.data_quality.spark.schema import validate_column

logger = get_logger(__name__)

def load_raw_data_landing_to_bronze(spark, s3_path, file_name, load_to_zone, catalog_name, schema_name, table_name, partition_col):
    '''
    Loads raw data from the landing zone in S3 to the medallion architecture zone in the lakehouse using Spark and Iceberg.

    Parameters:
        - spark: SparkSession object
        - s3_path: The S3 path where the raw data is stored in the landing zone
        - file_name: The name of the file to be loaded
        - load_to_zone: The target zone to load the data (e.g., "bronze", "silver", "gold")
        - catalog_name: The name of the Spark catalog configured for Iceberg
        - schema_name: The name of the schema (database) in the lakehouse where the table will be created
        - table_name: The name of the table to be created in the lakehouse
    '''

    s3_directory = f"s3a://{s3_path}/{file_name}"
    warehouse_iceberg_directory = f"s3a://lakehouse-{load_to_zone}-bucket/warehouse/{schema_name}/{table_name}/"

    # Extract the S&P 500 profile data from the landing zone
    logger.info(f"Extracting {file_name} data from landing zone at: {s3_directory}")
    df = spark.read.parquet(s3_directory)

    load_to_s3_lakehouse(spark ,df, catalog_name, schema_name, table_name, warehouse_iceberg_directory, partition_col)
    logger.info(f"Finished loading {file_name} data to {load_to_zone} zone")


# Load data into S3 with Table-Format (Iceberg)
def load_to_s3_lakehouse(spark ,df, catalog_name, schema_name, table_name, warehouse_iceberg_directory, partition_col):
    '''
    Loads the given DataFrame to the lakehouse using Spark and Iceberg.

    Parameters:
        - spark: SparkSession object
        - df: The DataFrame to be loaded
        - catalog_name: The name of the Spark catalog configured for Iceberg
        - schema_name: The name of the schema (database) in the lakehouse where the table will be created
        - table_name: The name of the table to be created in the lakehouse
    '''
    # Check the Partitioned-Columns
    logger.info(f"Starting Check the {df} and {partition_col} Validation.")
    validate_column(df, columns=partition_col)

    # Check the catalog_name.schema_name.table_name have exists or not
    if not spark.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}"):
        # If the table is not represent, the Spark will create a new table in S3 Lakehouse.
        logger.info(f"Data '{catalog_name}.{schema_name}.{table_name}' is not existing, then start create table")
        try:
            df.writeTo(f"{catalog_name}.{schema_name}.{table_name}") \
                .tableProperty("location", warehouse_iceberg_directory) \
                .partitionedBy(col(partition_col)) \
                .create()
            logger.info(f"Successfully loaded data to '{schema_name}.{table_name}' in the lakehouse")
        except Exception as e:
            logger.error(f"Error loading data to '{schema_name}.{table_name}': {e}")
            raise
    # If the table is exists, the Spark will replace with partition with ingested datetime
    else:
        logger.info(f"Data '{schema_name}.{table_name}' is existing, then append/replace the partition column/columns.")
        try:
            # Overwrite only date partition / if partition not apppear then append
            df.writeTo(f"{catalog_name}.{schema_name}.{table_name}") \
                .overwritePartitions()
            logger.info(f"Successfully loaded data to '{schema_name}.{table_name}' in the lakehouse")
        except Exception as e:
            logger.error(f"Error loading data to '{schema_name}.{table_name}': {e}")
            raise
