from pyspark.sql import SparkSession
from src.config.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# def generate_scd_merge_sql(source_table: str, target_table: str, key: list, scd_type: int) -> str:
#     '''
#     Generate SQL for Slowly Changing Dimension (SCD) merge operation based on the specified SCD type.

#     Parameters:
#     - source_table: The name of the source table containing new data.
#     - target_table: The name of the target table to be merged into.
#     - primary_key: The list of primary key columns used for matching records between source and target.
#     - scd_type: The type of SCD operation (1, 2, or 3).

#     Returns:
#     - A string containing the generated SQL statement for the SCD merge operation.
#     '''
#     if scd_type == 2:

#         sql = f"""
#         WITH source_updated_tbl AS (
#             SELECT source_table.*
#             FROM {source_table} source_table
#             JOIN {target_table} target_table
#             ON { ' AND '.join([f'target_table.{k} = source_table.{k}' for k in key])}
#             WHERE source_table.created_at > target_table.created_at
#                 AND target_table.is_current = TRUE
#         )
#         MERGE INTO {target_table} AS target_table
#         USING source_updated_tbl AS source_table

#         """


#     return sql


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
    try:
        # Step 1: Identify new and updated records
        source_updated_df = spark.sql(f"""
            SELECT source_table.*
            FROM {source_table} source_table
            LEFT JOIN {target_table} target_table
            ON { ' AND '.join([f'target_table.{k} = source_table.{k}' for k in key])}
            WHERE target_table.{key[0]} IS NULL -- New records
                OR (source_table.created_at > target_table.created_at AND target_table.is_current = TRUE) -- Updated records
        """)

        # Step 2: Mark existing records as expired
        spark.sql(f"""
            UPDATE {target_table}
            SET is_current = FALSE, expiry_date = CURRENT_DATE
            WHERE { ' AND '.join([f'{target_table}.{k} IN (SELECT {k} FROM source_updated_df)' for k in key])}
                AND is_current = TRUE
        """)

        # Step 3: Insert new records with effective and expiry dates
        source_updated_df.withColumn("is_current", lit(True)) \
                         .withColumn("effective_date", current_date()) \
                         .withColumn("expiry_date", lit(None).cast("date")) \
                         .write.insertInto(target_table, overwrite=False)

        logger.info("SCD Type 2 merge operation completed successfully.")
    except Exception as e:
        logger.error(f"SCD Type 2 merge operation failed: {e}")