import os
import yaml
import argparse
import json
import logging
import pandas as pd
from src.extractors.sqlserver import SqlServerExtractor
from src.extractors.postgres import PostgresExtractor
from src.loaders.snowflake import get_engine
from src.loaders.staging import StagingLoader
from src.transformers.sql_runner import SqlRunner

class JSONFormatter(logging.Formatter):
    """Custom logging formatter that outputs records as line-delimited JSON."""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging(json_logs=False, log_level=logging.INFO):
    """Configure system-wide logging with configurable JSON or console handlers."""
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        
    handler = logging.StreamHandler()
    if json_logs:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger = logging.getLogger("main")

def load_config(config_path="config/dummy.yml"):
    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()
        # Expand environment variables in the config file
        content = os.path.expandvars(content)
        return yaml.safe_load(content)

def format_fact_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format dataframe columns to exactly match expected_sales_fact.csv formatting."""
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]
    # Ensure correct columns and order
    cols = [
        "source_system",
        "source_transaction_id",
        "warehouse_transaction_id",
        "checkout_timestamp",
        "customer_id",
        "customer_country",
        "sku",
        "gross_amount_usd",
        "tax_amount_usd",
        "is_refunded"
    ]
    df = df[cols].copy()
    
    # Format float fields to two decimal places
    df["gross_amount_usd"] = df["gross_amount_usd"].apply(
        lambda x: f"{float(x):.2f}" if pd.notna(x) else ""
    )
    df["tax_amount_usd"] = df["tax_amount_usd"].apply(
        lambda x: f"{float(x):.2f}" if pd.notna(x) else ""
    )
    
    # Format timestamp to standard string format
    df["checkout_timestamp"] = df["checkout_timestamp"].apply(
        lambda x: str(x).split(".")[0] if pd.notna(x) else ""
    )
    
    # Format integer fields
    df["customer_id"] = df["customer_id"].apply(
        lambda x: str(int(float(x))) if pd.notna(x) else ""
    )
    df["source_transaction_id"] = df["source_transaction_id"].apply(
        lambda x: int(float(x))
    )
    
    # Format boolean to lowercase string
    df["is_refunded"] = df["is_refunded"].apply(
        lambda x: str(x).lower() if pd.notna(x) else "false"
    )
    
    # Fill NAs
    df["customer_country"] = df["customer_country"].fillna("").apply(lambda x: str(x) if x else "")
    df["sku"] = df["sku"].fillna("").apply(lambda x: str(x) if x else "")
    
    return df

def format_rejects_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format rejects dataframe columns to match expected_rejects.csv formatting."""
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]
    cols = ["source_system", "source_transaction_id", "reject_reason"]
    df = df[cols].copy()
    df["source_transaction_id"] = df["source_transaction_id"].apply(
        lambda x: int(float(x))
    )
    df["reject_reason"] = df["reject_reason"].fillna("").apply(str)
    return df

def main():
    parser = argparse.ArgumentParser(description="Enterprise Data Integration Pipeline")
    parser.add_argument("--config", default="config/dummy.yml", help="Path to config file")
    parser.add_argument("--use-snowflake", action="store_true", help="Force run on live Snowflake")
    parser.add_argument("--json-logs", action="store_true", help="Format logs as line-delimited JSON for production logging system ingestion")
    args = parser.parse_args()

    # Initialize Logging
    setup_logging(json_logs=args.json_logs)

    logger.info("Starting Enterprise Data Integration Pipeline...")
    
    try:
        # Load configuration
        config = load_config(args.config)

        # Initialize Engine (DuckDB or Snowflake)
        use_sf = args.use_snowflake
        
        if use_sf:
            logger.info("========================================")
            logger.info("   RUNNING ON SNOWFLAKE DATABASE ENGINE ")
            logger.info("========================================")
            logger.info("Initializing Snowflake connection...")
            sf_config = config.get("snowflake", {})
            engine = get_engine("snowflake", sf_config)
        else:
            logger.info("========================================")
            logger.info("     RUNNING ON LOCAL DUCKDB ENGINE     ")
            logger.info("========================================")
            logger.info("Initializing local DuckDB engine...")
            engine = get_engine("duckdb")

        # Initialize staging loader
        loader = StagingLoader(engine)
        loader.setup_schemas()

        # Extract SQL Server Data
        logger.info("Extracting SQL Server sales data...")
        sqlserver_path = config["paths"]["sqlserver_sales"]
        sqlserver_extractor = SqlServerExtractor(sqlserver_path)
        df_sqlserver = sqlserver_extractor.extract()

        # Extract Postgres Data
        logger.info("Extracting PostgreSQL sales data...")
        postgres_path = config["paths"]["postgres_sales"]
        postgres_extractor = PostgresExtractor(postgres_path)
        df_postgres = postgres_extractor.extract()

        # Extract/Read Exchange Rates
        logger.info("Extracting exchange rates...")
        exchange_rates_path = config["paths"]["exchange_rates"]
        df_exchange_rates = pd.read_csv(exchange_rates_path)

        # Load raw data into staging
        logger.info("Loading raw data into staging tables...")
        loader.load_raw_data(df_sqlserver, "STG_CORE_SALES")
        loader.load_raw_data(df_postgres, "STG_ACQUIRED_SALES")
        loader.load_raw_data(df_exchange_rates, "STG_EXCHANGE_RATES")

        # Run SQL transformations
        logger.info("Running staging transformations and warehouse merge...")
        runner = SqlRunner(engine)
        runner.run_script("src/sql/staging_core.sql")
        runner.run_script("src/sql/staging_acquired.sql")
        runner.run_script("src/sql/warehouse_merge.sql")

        # Fetch and verify results
        logger.info("Fetching results for validation...")
        if use_sf:
            # Use uppercase for Snowflake target
            fact_query = "SELECT * FROM DE_INTEGRATION.ANALYTICS.SALES_FACT ORDER BY source_system, source_transaction_id"
            rejects_query = "SELECT * FROM DE_INTEGRATION.ANALYTICS.SALES_REJECTS ORDER BY source_system, source_transaction_id"
        else:
            fact_query = "SELECT * FROM de_integration.analytics.sales_fact ORDER BY source_system, source_transaction_id"
            rejects_query = "SELECT * FROM de_integration.analytics.sales_rejects ORDER BY source_system, source_transaction_id"
            
        fact_df = engine.fetch_df(fact_query)
        rejects_df = engine.fetch_df(rejects_query)

        # Format DataFrames to match target CSV specifications
        fact_df_formatted = format_fact_df(fact_df)
        rejects_df_formatted = format_rejects_df(rejects_df)

        # Write files to data/output/ for manual inspections
        os.makedirs("data/output", exist_ok=True)
        fact_df_formatted.to_csv("data/output/sales_fact.csv", index=False)
        rejects_df_formatted.to_csv("data/output/rejects.csv", index=False)

        logger.info("Pipeline run completed successfully!")

        # Verify against expected values
        logger.info("Comparing outputs to expected fixtures...")
        expected_fact_path = config["paths"]["expected_sales_fact"]
        expected_rejects_path = config["paths"]["expected_rejects"]
        
        expected_fact_df = format_fact_df(pd.read_csv(expected_fact_path))
        expected_rejects_df = format_rejects_df(pd.read_csv(expected_rejects_path))

        # Perform sorting to guarantee alignment during print/comparison
        fact_df_formatted = fact_df_formatted.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)
        expected_fact_df = expected_fact_df.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)
        
        rejects_df_formatted = rejects_df_formatted.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)
        expected_rejects_df = expected_rejects_df.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)

        fact_matches = fact_df_formatted.equals(expected_fact_df)
        rejects_matches = rejects_df_formatted.equals(expected_rejects_df)

        if fact_matches:
            logger.info("SUCCESS: Sales Fact matches the expected output perfectly!")
        else:
            logger.error("ERROR: Sales Fact does NOT match the expected output!")
            logger.error(f"Got:\n{fact_df_formatted}")
            logger.error(f"Expected:\n{expected_fact_df}")

        if rejects_matches:
            logger.info("SUCCESS: Rejects table matches the expected rejects perfectly!")
        else:
            logger.error("ERROR: Rejects table does NOT match the expected rejects!")
            logger.error(f"Got:\n{rejects_df_formatted}")
            logger.error(f"Expected:\n{expected_rejects_df}")
            
    except Exception as e:
        logger.error(f"Pipeline run aborted due to errors: {e}", exc_info=True)
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
