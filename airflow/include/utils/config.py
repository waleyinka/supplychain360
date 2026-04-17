# airflow/include/utils/config.py

import os
from dotenv import load_dotenv
from include.utils.ssm_config import get_db_parameters

# Load environment variables from .env file
load_dotenv()

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
RAW_S3_BUCKET = os.getenv("RAW_S3_BUCKET")
PROJECT_S3_BUCKET_NAME=os.getenv("PROJECT_S3_BUCKET_NAME")

# Google Sheets Configuration
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RANGE_NAME = os.getenv("RANGE_NAME")
GOOGLE_SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "service_account.json")

# Snowflake Configuration
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_STORAGE_INTEGRATION = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION")

# Logging Configuration
LOG_DIRECTORY = os.getenv("LOG_DIRECTORY", "logs")
LOG_FILE_NAME = os.getenv("LOG_FILE_NAME", "supplychain360.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# dbt Configuration
DBT_JOB_ID = os.getenv("DBT_JOB_ID")

# FETCH DB CREDENTIALS FROM SSM
def get_source_db_config():
    base_path = "/supplychain360/db"
    params = get_db_parameters(base_path)
    
    return {
        "HOST": params.get("host"),
        "PORT": params.get("port", "5432"),
        "DBNAME": params.get("dbname"),
        "USER": params.get("user"),
        "PASSWORD": params.get("password"),
    }
