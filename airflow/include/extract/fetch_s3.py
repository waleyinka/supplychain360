# airflow/dags/include/extract/fetch_s3.py

"""Utilities for reading project datasets from the Source S3 Bucket."""
import pandas as pd
import boto3
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import include.utils.config as config
from include.utils.logger import get_logger
from include.utils.retry import retry

logger = get_logger(__name__)

@retry(Exception, retries=3, delay=5)
def fetch_s3_dataset(path: str, file_type: str = "csv") -> pd.DataFrame:
    """
    Reads CSV or JSON from S3 using awswrangler.
    Path should be the prefix or specific file.
    """
    import awswrangler as wr
    
    wr.config.engine = "python"
    
    s3_path = f"s3://{config.PROJECT_S3_BUCKET_NAME}/{path}"
    logger.info(f"Fetching {file_type} data from {s3_path}")
    
    aws_hook = AwsBaseHook(aws_conn_id="aws_source", client_type="s3")
    credentials = aws_hook.get_credentials()
    
    session = boto3.Session(
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
        region_name=config.AWS_REGION,
    )
    
    if file_type == "csv":
        return wr.s3.read_csv(path=s3_path, boto3_session=session)
    elif file_type == "json":
        return wr.s3.read_json(path=s3_path, boto3_session=session)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")