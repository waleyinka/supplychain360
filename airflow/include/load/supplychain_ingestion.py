# airflow/include/load/supplychain_ingestion.py

"""High-level orchestrator for all SupplyChain360 data sources."""
from include.extract.google_sheets import fetch_sheets_data
from include.extract.postgres_sales import fetch_new_store_sales
from include.utils.generic_ingestor import standard_ingestion_flow
from include.utils.logger import get_logger

logger = get_logger(__name__)

def ingest_master_data():
    """Logic for Google Sheets -> S3."""
    logger.info("Starting Master Data Ingestion (Google Sheets)...")
    df = fetch_sheets_data()
    standard_ingestion_flow(
        df=df,
        source_system="google_sheets",
        source_object="store_locations",
        s3_key="raw/master_data/store_locations.parquet"
    )

def ingest_operational_data(already_loaded_tables=None):
    """Logic for Postgres Sales -> S3."""
    logger.info("Starting Operational Data Ingestion (Postgres Sales)...")
    # This logic calls your fetch_new_store_sales which discovered new tables
    df = fetch_new_store_sales(already_loaded_tables=already_loaded_tables)
    
    if not df.empty:
        standard_ingestion_flow(
            df=df,
            source_system="project_postgres",
            source_object="daily_sales_batch",
            s3_key="raw/store_sales/latest_batch.parquet"
        )