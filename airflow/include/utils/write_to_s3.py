# airflow/include/utils/write_to_s3.py

"""Utility for writing DataFrames to S3 using AWS Data Wrangler."""

import boto3
import pandas as pd
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from include.utils.logger import get_logger
import include.utils.config as config

import awswrangler as wr

wr.config.engine = "python"

logger = get_logger(__name__)

def write_parquet(df: pd.DataFrame, object_key: str) -> str:
    """Writes a DataFrame to the personal RAW S3 bucket in Parquet format.

    Args:
        df: DataFrame to persist.
        object_key: Path inside the bucket (e.g., 'raw/sales/data.parquet').
        compression: Parquet compression algorithm.

    Returns:
        str: The full S3 Path of the created object.
    """
    aws_hook = AwsBaseHook(aws_conn_id="aws_destination", client_type="s3")
    credentials = aws_hook.get_credentials()
    
    session = boto3.Session(
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=aws_hook.region_name
        )
        
        
    s3_path = f"s3://{config.RAW_S3_BUCKET}/{object_key}"
        
    wr.s3.to_parquet(
            df=df,
            path=s3_path,
            boto3_session=session,
            index=False
        )
    
    logger.info(f"Successfully wrote {len(df)} rows to {s3_path}")
        
    return s3_path