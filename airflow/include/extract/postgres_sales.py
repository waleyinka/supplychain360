# airflow/include/extract/postgres_sales.py

"""
Extract store sales incrementally from the source Postgres database.

Incremental logic:
- The source creates a new sales table each day.
- We ingest only tables that have not been loaded yet.

Features:
- Discovery of dynamic daily tables (sales_YYYY_MM_DD).
- Resilience via SQLAlchemy connection pooling.
- Integration with project-wide logging and metadata utilities.
"""

from __future__ import annotations

from typing import List, Optional, Set
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import include.utils.config as config
from include.utils.logger import get_logger
from include.utils.metadata import add_metadata
from include.utils.retry import retry


logger = get_logger(__name__)

_engine: Optional[Engine] = None

def get_source_engine() -> Engine:
    """
    Create a SQLAlchemy engine with connection pooling for
    the source Postgres database..
    """
    global _engine
    if _engine is None:
        db = config.get_source_db_config()
        
        if not db["HOST"]:
            raise ValueError("Database HOST retrieved from SSM is None. Check SSM paths.")
        
        url = (
            f"postgresql+psycopg2://{db['USER']}:{db['PASSWORD']}"
            f"@{db['HOST']}:{db['PORT']}/{db['DBNAME']}"
        )

        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=2,
            pool_recycle=1800,
        )
    
    return _engine


def list_sales_tables(schema_name: str = "public") -> List[str]:
    """
    Lists source sales tables in the information schema.
    
    Args:
        table_name: Name of the daily table (e.g., sales_2026_03_10).

    Returns:
        List[str]: Matching table names sorted alphabetically.
    """
    logger.info("Listing sales tables from source schema %s", schema_name)
    
    engine = get_source_engine()

    query = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema_name
          AND lower(table_name) LIKE 'sales_%'
        ORDER BY table_name
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, {"schema_name": schema_name}).fetchall()
            
    table_names = [row[0] for row in rows]
    logger.info(f"Discovered {len(table_names)} sales tables.")
    
    return table_names


def get_new_sales_tables(
    already_loaded_tables: Optional[Set[str]] = None,
    schema_name: str = "public",
) -> List[str]:
    """
    Filters and return only sales tables that have not yet been ingested.

    Args:
        already_loaded_tables: Set of table names already ingested.
        schema_name: Source schema name.

    Returns:
        List[str]: New table names to ingest.
    """
    already_loaded_tables = already_loaded_tables or set()
    source_tables = list_sales_tables(schema_name=schema_name)

    new_tables = [table for table in source_tables if table not in already_loaded_tables]
    
    logger.info(
        "Found %s new sales tables after excluding %s already loaded tables",
        len(new_tables),
        len(already_loaded_tables),
    )
    
    return new_tables


@retry(Exception, retries=2, delay=5)
def fetch_sales_table(table_name: str, schema_name: str = "public") -> pd.DataFrame:
    """
    Fetches all rows from a single source sales table and attaches audit metadata.

    Args:
        table_name: Source table name.
        schema_name: Source schema name.

    Returns:
        pd.DataFrame: Sales rows from the table with source metadata columns.
    """
    if not table_name.lower().startswith("sales_"):
        raise ValueError(f"Unexpected table name: {table_name}")
    
    logger.info("Fetching source sales table %s.%s", schema_name, table_name)
    
    engine = get_source_engine()
    
    query = text(f'SELECT * FROM "{schema_name}"."{table_name}"')

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    df = add_metadata(df, source_system="sales_postgres", source_object=table_name)
    
    logger.info(
        "Fetched %s rows and %s columns from %s",
        len(df),
        len(df.columns),
        table_name,
    )
        
    return df


def fetch_new_store_sales(
    already_loaded_tables: Optional[Set[str]] = None,
    schema_name: str = "public",
) -> pd.DataFrame:
    """
    Fetch rows from all new daily sales tables.

    Args:
        already_loaded_tables: Set of already ingested source table names.
        schema_name: Source schema name.

    Returns:
        pd.DataFrame: Combined rows from all new sales tables.
    """
    new_tables = get_new_sales_tables(
        already_loaded_tables=already_loaded_tables,
        schema_name=schema_name,
    )

    if not new_tables:
        logger.info("No new sales tables found")
        return pd.DataFrame()

    frames = []
    for table_name in new_tables:
        df = fetch_sales_table(table_name=table_name, schema_name=schema_name)
        frames.append(df)
        
    combined_df = pd.concat(frames, ignore_index=True)

    logger.info(
        "Fetched total %s rows across %s new sales tables",
        len(combined_df),
        len(new_tables),
    )

    return combined_df