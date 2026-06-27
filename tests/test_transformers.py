import os
import pandas as pd
import pytest
from src.loaders.snowflake import get_engine
from src.loaders.staging import StagingLoader
from src.transformers.sql_runner import SqlRunner
from src.extractors.sqlserver import SqlServerExtractor
from src.extractors.postgres import PostgresExtractor
from main import format_fact_df, format_rejects_df, load_config

def test_duckdb_custom_functions():
    engine = get_engine("duckdb")
    
    # Test try_parse_json
    res_valid = engine.fetch_df("SELECT try_parse_json('{\"a\": 1}') AS val")
    assert res_valid.iloc[0]["val"] == '{"a": 1}'
    
    res_invalid = engine.fetch_df("SELECT try_parse_json('{bad') AS val")
    assert res_invalid.iloc[0]["val"] is None or pd.isna(res_invalid.iloc[0]["val"])

    # Test get
    res_get = engine.fetch_df("SELECT get('{\"id\": 42}', 'id') AS val")
    assert str(res_get.iloc[0]["val"]) == "42"

    # Test try_to_timestamp
    res_ts = engine.fetch_df("SELECT try_to_timestamp('10/01/2026 15:30:00', 'DD/MM/YYYY HH24:MI:SS') AS val")
    assert str(res_ts.iloc[0]["val"]) == "2026-01-10 15:30:00"

def test_pipeline_e2e_local():
    config = load_config("config/dummy.yml")
    engine = get_engine("duckdb")
    loader = StagingLoader(engine)
    loader.setup_schemas()

    # Extract
    df_sqlserver = SqlServerExtractor(config["paths"]["sqlserver_sales"]).extract()
    df_postgres = PostgresExtractor(config["paths"]["postgres_sales"]).extract()
    df_exchange_rates = pd.read_csv(config["paths"]["exchange_rates"])

    # Stage
    loader.load_raw_data(df_sqlserver, "STG_CORE_SALES")
    loader.load_raw_data(df_postgres, "STG_ACQUIRED_SALES")
    loader.load_raw_data(df_exchange_rates, "STG_EXCHANGE_RATES")

    # Transform
    runner = SqlRunner(engine)
    runner.run_script("src/sql/staging_core.sql")
    runner.run_script("src/sql/staging_acquired.sql")
    runner.run_script("src/sql/warehouse_merge.sql")

    # Fetch
    fact_df = engine.fetch_df("SELECT * FROM de_integration.analytics.sales_fact")
    rejects_df = engine.fetch_df("SELECT * FROM de_integration.analytics.sales_rejects")

    # Format
    fact_df_formatted = format_fact_df(fact_df)
    rejects_df_formatted = format_rejects_df(rejects_df)

    # Sort
    fact_df_formatted = fact_df_formatted.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)
    rejects_df_formatted = rejects_df_formatted.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)

    # Read Expected
    expected_fact_df = format_fact_df(pd.read_csv(config["paths"]["expected_sales_fact"]))
    expected_rejects_df = format_rejects_df(pd.read_csv(config["paths"]["expected_rejects"]))

    expected_fact_df = expected_fact_df.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)
    expected_rejects_df = expected_rejects_df.sort_values(by=["source_system", "source_transaction_id"]).reset_index(drop=True)

    # Assert character-perfect matching
    pd.testing.assert_frame_equal(fact_df_formatted, expected_fact_df)
    pd.testing.assert_frame_equal(rejects_df_formatted, expected_rejects_df)

def test_retry_decorator():
    from src.loaders.snowflake import retry
    
    attempts = 0
    
    @retry(ValueError, tries=3, delay=0.01, backoff=1, jitter=False)
    def fail_twice():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("Transient error")
        return "success"
        
    res = fail_twice()
    assert res == "success"
    assert attempts == 3
    
    # Test retry reaches limit and propagates exception
    attempts_prop = 0
    @retry(ValueError, tries=2, delay=0.01, backoff=1, jitter=False)
    def fail_always():
        nonlocal attempts_prop
        attempts_prop += 1
        raise ValueError("Permanent error")
        
    with pytest.raises(ValueError):
        fail_always()
    assert attempts_prop == 2

def test_json_formatter():
    import json
    import logging
    from main import JSONFormatter
    
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message with format: %s",
        args=("hello",),
        exc_info=None
    )
    
    json_str = formatter.format(record)
    data = json.loads(json_str)
    
    assert data["level"] == "INFO"
    assert data["logger"] == "test_logger"
    assert data["message"] == "Test message with format: hello"
    assert "timestamp" in data

