from src.config.logger import get_logger

logger = get_logger(__name__)

# Validate Column in Lakehouse
def validate_column(df, columns):
    if isinstance(columns, str):
        columns = [columns]

    missing_cols = [col for col in columns if col not in df.columns]

    if missing_cols:
        logger.error(f"Missing columns: {missing_cols}")
        raise ValueError(f"Missing columns in DataFrame: {missing_cols}")

    logger.info(f"All columns validated successfully: {columns}")