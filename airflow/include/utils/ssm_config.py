# include/utils/ssm_config.py

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3

def get_db_parameters(base_path: str) -> dict:
    aws_hook = AwsBaseHook(aws_conn_id="aws_source", client_type="ssm")
    credentials = aws_hook.get_credentials()

    session = boto3.Session(
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
        region_name=aws_hook.region_name or "eu-west-2",
    )

    ssm = session.client("ssm")
    paginator = ssm.get_paginator("get_parameters_by_path")

    params = {}
    for page in paginator.paginate(Path=base_path, WithDecryption=True):
        for p in page["Parameters"]:
            key = p["Name"].replace(base_path, "").lstrip("/")
            params[key] = p["Value"]

    return params