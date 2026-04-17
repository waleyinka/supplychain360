# airflow/dags/include/utils/aws_sessions.py

"""AWS Hook helpers for Project (Source) and Personal (Destination) accounts."""

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from botocore.client import BaseClient


def get_project_s3_client() -> BaseClient:
    """Returns an S3 client for the SOURCE project account.
    
    Connection ID 'aws_source' must be configured in Airflow.
    """
    return AwsBaseHook(aws_conn_id="aws_source").get_client_type("s3")

def get_my_s3_hook() -> AwsBaseHook:
    """Returns the AWS Hook for the DESTINATION personal account.
    
    Used by awswrangler and other high-level AWS tools.
    """
    return AwsBaseHook(aws_conn_id="aws_destination")