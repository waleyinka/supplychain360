# ingestion/transform/metadata.py

from datetime import datetime
import pandas as pd


def add_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Add ingestion metadata.

    Args:
        df: Input dataframe
        source: Source system name

    Returns:
        pd.DataFrame
    """
    df["_ingestion_timestamp"] = datetime.utcnow()
    df["_source"] = source
    return df