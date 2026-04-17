# include/load/ingest_store_sales.py

"""
Incremental + Idempotent ingestion for daily sales tables.
Target: S3 (Raw) -> Snowflake (RAW Schema).
"""
from __future__ import annotations

import include.utils.config as config
from typing import List, Set

import include.utils.config as config
from include.extract.postgres_sales import list_sales_tables, fetch_sales_table
from include.utils.metadata import add_metadata
from include.utils.write_to_s3 import write_parquet
from include.utils.logger import get_logger
from include.load.snowflake_loader import get_snowflake_conn, load_parquet_to_snowflake

logger = get_logger(__name__)


def get_ingested_tables_from_snowflake() -> Set[str]:
    """
    Queries Snowflake to find which source tables have already been loaded.
    This replaces the old Supabase metadata check.
    """
    query = "SELECT DISTINCT _source_object FROM RAW.STORE_SALES"
    
    try:
        with get_snowflake_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES LIKE 'STORE_SALES' IN SCHEMA RAW")
            if not cursor.fetchone():
                return set()
            
            cursor.execute(query)
            results = cursor.fetchall()
            return {row[0] for row in results}
    
    except Exception as e:
        logger.warning(f"Could not fetch metadata from Snowflake (likely table doesn't exist): {e}")
        return set()


def ingest_store_sales():
    """
    Orchestrates the movement of sales data from Postgres to S3, 
    then triggers the Snowflake COPY command.
    """
    logger.info("Starting Incremental Sales Ingestion to Snowflake")

    # 1. Discovery
    all_source_tables = list_sales_tables()
    already_loaded = get_ingested_tables_from_snowflake()
    
    new_tables = [t for t in all_source_tables if t not in already_loaded]
    
    if not new_tables:
        logger.info("No new sales tables to ingest.")
        return

    for table_name in new_tables:
        try:
            logger.info(f"Processing {table_name}...")
            
            # 2. Extract & Enrich
            df = fetch_sales_table(table_name)
            # add_metadata standardizes columns and adds audit timestamps
            df_raw = add_metadata(df, "project_postgres", table_name)
            
            # 3. Write to S3 (Use hive-style partitioning for S3)
            object_key = f"store_sales/source_table={table_name}/data.parquet"
            s3_uri = write_parquet(df_raw, object_key)
            
            # 4. Load to Snowflake
            load_parquet_to_snowflake(
                dataset_name="store_sales",
                s3_prefix=f"store_sales/source_table={table_name}/",
                target_table="STORE_SALES"
            )
            
            logger.info(f"Successfully moved {table_name} to Snowflake RAW.STORE_SALES")

        except Exception as e:
            logger.error(f"Failed to ingest {table_name}: {e}")
            continue

if __name__ == "__main__":
    ingest_store_sales()