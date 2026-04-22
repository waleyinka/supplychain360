

# airflow/include/utils/ssm_config.py

"""Fetches database credentials from AWS Systems Manager (SSM) Parameter Store."""

import boto3
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from typing import Dict
from botocore.exceptions import ClientError
from include.utils.logger import get_logger


logger = get_logger(__name__)

def get_db_parameters(base_path: str = "/supplychain360/db", region: str = "eu-west-2") -> Dict[str, str]:
    """Retrieves DB credentials from SSM Parameter Store recursively.

    Args:
        base_path: The SSM path prefix.
        region: AWS region.

    Returns:
        Dict: Mapping of credential keys to values.
    """
    try:
        aws_hook = AwsBaseHook(aws_conn_id="aws_source", client_type="ssm", region_name="eu-west-2")
        ssm = aws_hook.get_conn()
        
        paginator = ssm.get_paginator("get_parameters_by_path")
        
        params = {}
        for page in paginator.paginate(Path=base_path, WithDecryption=True):
            for p in page.get("Parameters", []):
                key = p["Name"].split("/")[-1].upper()
                params[key] = p["Value"]
        
        return {
            "HOST": params.get("HOST"),
            "PORT": params.get("PORT", "5432"),
            "DBNAME": params.get("DBNAME"),
            "USER": params.get("USER"),
            "PASSWORD": params.get("PASSWORD")
        }
        
    except ClientError as e:
        logger.error(f"Failed to fetch SSM parameters from {base_path}: {e}")
        raise