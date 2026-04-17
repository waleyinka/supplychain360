# airflow/include/utils/metadata.py

"""Helpers for attaching ingestion metadata to DataFrames."""

import pandas as pd
from datetime import datetime, timezone

def add_metadata(df: pd.DataFrame, source_system: str, source_object: str) -> pd.DataFrame:
    """Appends audit metadata columns to a DataFrame.

    Args:
        df: Input DataFrame.
        source_system: Origin system (e.g., 'postgres_sales').
        source_object: Origin object (e.g., 'sales_2026_01_01').

    Returns:
        pd.DataFrame: Enriched DataFrame.
    """
    enriched_df = df.copy()
    now = datetime.now(timezone.utc)
    
    # Audit Columns
    enriched_df["_ingested_at"] = now
    enriched_df["_source_system"] = source_system
    enriched_df["_source_object"] = source_object
    
    # Clean column names to standard (lowercase, no spaces)
    enriched_df.columns = [c.lower().replace(" ", "_").strip() for c in enriched_df.columns]
    
    return enriched_df