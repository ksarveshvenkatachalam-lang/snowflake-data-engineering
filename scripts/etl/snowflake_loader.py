"""Snowflake Data Loader

This module provides utilities for loading data into Snowflake.
"""

import os
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from typing import Optional


class SnowflakeLoader:
    """A class to handle Snowflake data loading operations."""
    
    def __init__(self):
        """Initialize Snowflake connection using environment variables."""
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA', 'RAW')
        self.conn = None

    def connect(self):
        """Establish connection to Snowflake."""
        self.conn = connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        return self.conn

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None
    ) -> bool:
        """Load a pandas DataFrame into Snowflake."""
        if self.conn is None:
            self.connect()
        
        target_schema = schema or self.schema
        success, _, nrows, _ = write_pandas(
            self.conn,
            df,
            table_name,
            schema=target_schema,
            auto_create_table=True
        )
        print(f"Loaded {nrows} rows to {target_schema}.{table_name}")
        return success

    def close(self):
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    # Example usage
    loader = SnowflakeLoader()
    print("Snowflake Loader initialized successfully")
