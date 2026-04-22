# airflow/include/utils/generic_ingestor.py

"""Generic logic to standardize the Ingestion -> S3 flow."""
import pandas as pd
from include.utils.metadata import add_metadata
from include.utils.write_to_s3 import write_parquet
from include.utils.logger import get_logger

logger = get_logger(__name__)

def standard_ingestion_flow(
    df: pd.DataFrame, 
    source_system: str, 
    source_object: str, 
    s3_key: str
) -> str:
    """Standardizes enrichment and persistence to S3."""
    if df.empty:
        logger.warning(f"No data found for {source_object}. Skipping.")
        return ""

    # Enrich with audit columns (from metadata.py)
    enriched_df = add_metadata(df, source_system, source_object)
    
    # Persist to S3 (from write_to_s3.py)
    s3_uri = write_parquet(enriched_df, s3_key)
    
    return s3_uri