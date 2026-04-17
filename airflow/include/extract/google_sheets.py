# airflow/include/extract/google_sheets.py

"""
Utilities for reading data from Google Sheets.
"""
import pandas as pd
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from include.utils.retry import retry
from include.utils.logger import get_logger
from include.utils.metadata import add_metadata
import include.utils.config as config

logger = get_logger(__name__)

@retry(Exception, retries=3, delay=5)
def fetch_sheets_data(
    spreadsheet_id: str = config.SPREADSHEET_ID,
    range_name: str = config.RANGE_NAME,
    gcp_conn_id: str = "google_cloud_default"
) -> pd.DataFrame:
    """
    Fetches a Google Sheet range using Airflow's native GSheetsHook.
    
    This avoids manually building the service object and handles 
    credentials via the Airflow Connection UI.
    """
    logger.info(f"Fetching from Google Sheet: {spreadsheet_id} | Range: {range_name}")
    
    try:
        hook = GSheetsHook(gcp_conn_id=gcp_conn_id)
        
        values = hook.get_values(spreadsheet_id=spreadsheet_id, range_=range_name)
        
        if not values or len(values) < 1:
            logger.warning("No data found in the specified range.")
            return pd.DataFrame()
            
        # Extract header and data
        header = values[0]
        data = values[1:]
        
        df = pd.DataFrame(data, columns=header)
        
        # Apply standard naming convention (lowercase, no spaces)
        df.columns = [c.lower().replace(" ", "_").strip() for c in df.columns]
        
        # Apply standard metadata and naming (via add_metadata and internal cleaning)
        df = add_metadata(
            df, 
            source_system="google_sheets", 
            source_object=f"{spreadsheet_id}_{range_name}"
        )
        
        logger.info(f"Successfully extracted {len(df)} rows.")
        return df

    except Exception as e:
        logger.error(f"Error extracting from Google Sheets: {e}")
        raise