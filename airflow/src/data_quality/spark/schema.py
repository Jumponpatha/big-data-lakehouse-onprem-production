from src.config.logger import get_logger

logger = get_logger(__name__)

# Validate Column in Lakehouse
def validate_column(df, columns):
    # Normalize to list
    if isinstance(columns, str):
        columns = [columns]

    # Loop Check each Columns
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")
        else:
            raise logger.info(f"Column '{col}' are/is validate in DataFrame")