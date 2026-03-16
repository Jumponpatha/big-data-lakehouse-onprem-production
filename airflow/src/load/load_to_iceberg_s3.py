from src.config.logger import get_logger

logger = get_logger(__name__)

def load_raw_data_landing_to_bronze(spark, s3_path, file_name, load_to_zone, catalog_name, schema_name, table_name):
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
    df = spark.read.parquet(s3_directory, header=True, inferSchema=True)

    try:
        logger.info(f"Loading {file_name} data to {load_to_zone} zone in the lakehouse at: {warehouse_iceberg_directory}")
        df.writeTo(f"{catalog_name}.{schema_name}.{table_name}") \
                .tableProperty("location", warehouse_iceberg_directory) \
                .createOrReplace()
        logger.info(f"Successfully loaded '{schema_name}.{table_name}' data to {load_to_zone} zone at {warehouse_iceberg_directory}")
    except Exception as e:
        logger.error(f"Error loading {file_name} data to {load_to_zone} zone: {e}")
        raise
    logger.info(f"Finished loading {file_name} data to {load_to_zone} zone")