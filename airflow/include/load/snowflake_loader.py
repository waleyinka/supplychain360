# airflow/include/load/snowflake_loader.py

"""Handles the Load phase: S3 -> Snowflake."""
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.utils.logger import get_logger
import include.utils.config as config

logger = get_logger(__name__)

def get_snowflake_conn():
    """Helper to get a connection object from the SnowflakeHook."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    return hook.get_conn()

def load_parquet_to_snowflake(
    dataset_name: str, 
    s3_prefix: str, 
    target_table: str,
    schema: str = "RAW"
):
    """Executes Snowflake COPY INTO command for a specific S3 prefix."""
    
    copy_sql = f"""
    COPY INTO {config.SNOWFLAKE_DATABASE}.{schema}.{target_table}
    FROM @{config.SNOWFLAKE_STORAGE_INTEGRATION}/{s3_prefix}
    FILE_FORMAT = (TYPE = 'PARQUET' BINARY_AS_TEXT = FALSE)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE;
    """
    
    try:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        hook.run(copy_sql)
        logger.info(f"Snowflake COPY completed for {dataset_name} at {s3_prefix}")
    
    except Exception as e:
        logger.error(f"Snowflake Load Error for {dataset_name}: {e}")
        raise