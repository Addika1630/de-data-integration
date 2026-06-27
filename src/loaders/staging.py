import logging
import pandas as pd
from src.loaders.snowflake import DBEngine

logger = logging.getLogger(__name__)

class StagingLoader:
    """Loader to prepare schemas and load raw source data into Snowflake (or local Mock DB)."""

    def __init__(self, engine: DBEngine):
        self.engine = engine

    def setup_schemas(self):
        """Creates RAW and ANALYTICS schemas if they do not exist."""
        logger.info("Setting up database schemas (RAW, ANALYTICS)...")
        try:
            # For Snowflake connection, ensure the DE_INTEGRATION database exists
            if "SnowflakeEngine" in str(type(self.engine)):
                logger.info("Snowflake engine detected. Creating target database DE_INTEGRATION if not exists.")
                self.engine.execute("CREATE DATABASE IF NOT EXISTS DE_INTEGRATION;")
            self.engine.execute("CREATE SCHEMA IF NOT EXISTS DE_INTEGRATION.RAW;")
            self.engine.execute("CREATE SCHEMA IF NOT EXISTS DE_INTEGRATION.ANALYTICS;")
            # Drop stale staging tables to force fresh auto-creation with uppercase column names
            logger.info("Dropping older staging tables to ensure clean schema state...")
            self.engine.execute("DROP TABLE IF EXISTS DE_INTEGRATION.RAW.STG_CORE_SALES;")
            self.engine.execute("DROP TABLE IF EXISTS DE_INTEGRATION.RAW.STG_ACQUIRED_SALES;")
            self.engine.execute("DROP TABLE IF EXISTS DE_INTEGRATION.RAW.STG_EXCHANGE_RATES;")
            logger.info("Schema setup and staging table clean-up completed successfully.")
        except Exception as e:
            logger.error(f"Error during schema setup: {e}", exc_info=True)
            raise

    def load_raw_data(self, df: pd.DataFrame, table_name: str):
        """Loads a source DataFrame into a RAW staging table."""
        logger.info(f"Preparing to stage dataframe with {len(df)} rows into {table_name}...")
        try:
            # Convert columns to uppercase for Snowflake case-insensitivity compatibility
            df_upper = df.copy()
            df_upper.columns = [c.upper() for c in df_upper.columns]
            self.engine.load_df(df_upper, table_name, schema="RAW")
            logger.info(f"Dataframe loaded successfully into raw staging table {table_name}.")
        except Exception as e:
            logger.error(f"Failed staging raw dataframe into table {table_name}: {e}", exc_info=True)
            raise

