import boto3
from io import BytesIO, StringIO
from datetime import datetime
from airflow.sdk.bases.hook import BaseHook
from src.config.logger import get_logger
from botocore.exceptions import NoCredentialsError

# Initialize logger
logger = get_logger(__name__)

# Get MinIO S3 client
def get_minio_s3_client():
    '''Creates and returns a MinIO S3 client using Airflow connection.'''
    try:
        # Retrieve MinIO S3 connection details from Airflow connections
        conn = BaseHook.get_connection("minio_s3")
        extra = conn.extra_dejson
        endpoint = extra.get("endpoint_url")

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name="us-east-1"
        )
        logger.info("MinIO S3 client created successfully.")
        return s3_client
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure your AWS credentials.")
        raise
    except Exception as e:
        logger.error(f"An error occurred while creating MinIO S3 client: {e}")
        raise


def load_data_profiles_to_s3(df, file_name, bucket_name, folder_name, file_format="parquet"):
    '''Uploads the given DataFrame to MinIO S3 in the specified format.'''
    s3 = get_minio_s3_client()

    # Construct the full file path in S3
    file_name = f"{folder_name}/{file_name}_{datetime.now().strftime('%Y%m%d')}.{file_format}"

    # Convert DataFrame to the specified format and write to a buffer
    if file_format == "parquet":
        # Use BytesIO for binary formats like Parquet
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")

    elif file_format == "csv":
        # Use StringIO for text formats like CSV
        buffer = StringIO()
        df.to_csv(buffer, index=False)

    # Upload the buffer content to S3
    try:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())
        logger.info(f"Data uploaded to S3 bucket '{bucket_name}' with path '{file_name}'")
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure your AWS credentials.")
