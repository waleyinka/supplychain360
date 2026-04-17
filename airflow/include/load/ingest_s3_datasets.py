# airflow/dags/include/load/ingest_s3_datasets.py

"""Orchestrates S3-to-S3 ingestion for SupplyChain360."""
from include.extract.fetch_s3 import fetch_s3_dataset
from include.utils.generic_ingestor import standard_ingestion_flow
from include.utils.logger import get_logger
from include.load.snowflake_loader import load_parquet_to_snowflake

logger = get_logger(__name__)

# Define the datasets: { dataset_name: (source_path, file_type, target_table) }
S3_DATASETS = {
    "suppliers": ("raw/suppliers/suppliers.csv", "csv", "SUPPLIERS"),
    "warehouses": ("raw/warehouses/warehouses.csv", "csv", "WAREHOUSES"),
    "products": ("raw/products/products.csv", "csv", "PRODUCTS"),
    "inventory": ("raw/inventory/", "csv", "INVENTORY_SNAPSHOTS"),
    "shipments": ("raw/shipments/", "json", "SHIPMENT_LOGS")
}

def ingest_all_s3_source_data():
    """Loops through all S3 datasets, moves to personal S3, and loads Snowflake."""
    for name, (path, ftype, table) in S3_DATASETS.items():
        try:
            logger.info(f"Processing S3 Dataset: {name}")
            
            # 1. Extract
            df = fetch_s3_dataset(path, ftype)
            
            # 2. Transform & Load to Personal S3
            # We partition by name to keep the Raw bucket organized
            s3_key = f"raw/{name}/source_object={name}/data.parquet"
            standard_ingestion_flow(
                df=df,
                source_system="aws_source_s3",
                source_object=name,
                s3_key=s3_key
            )
            
            # 3. Load to Snowflake
            load_parquet_to_snowflake(
                dataset_name=name,
                s3_prefix=f"{name}/source_object={name}/",
                target_table=table
            )
            
        except Exception as e:
            logger.error(f"Failed to ingest S3 dataset {name}: {e}")
            continue