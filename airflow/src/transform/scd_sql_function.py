from pyspark.sql import SparkSession
from src.config.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

def generate_scd_merge_sql(source_table: str, target_table: str, key: list, scd_type: int) -> str:
    '''
    Generate SQL for Slowly Changing Dimension (SCD) merge operation based on the specified SCD type.

    Parameters:
    - source_table: The name of the source table containing new data.
    - target_table: The name of the target table to be merged into.
    - primary_key: The list of primary key columns used for matching records between source and target.
    - scd_type: The type of SCD operation (1, 2, or 3).

    Returns:
    - A string containing the generated SQL statement for the SCD merge operation.
    '''
    if scd_type == 2:

        sql = f"""
        WITH source_updated_tbl AS (
            SELECT source_table.*
            FROM {source_table} source_table
            JOIN {target_table} target_table
            ON { ' AND '.join([f'target_table.{k} = source_table.{k}' for k in key])}
            WHERE source_table.created_at > target_table.created_at
                AND target_table.is_current = TRUE
        )
        MERGE INTO {target_table} AS target_table
        USING source_updated_tbl AS source_table

        """


    return sql