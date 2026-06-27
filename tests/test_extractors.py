import pandas as pd
from src.extractors.sqlserver import SqlServerExtractor
from src.extractors.postgres import PostgresExtractor

def test_sqlserver_extractor():
    extractor = SqlServerExtractor("data/raw/sqlserver_mock.csv")
    df = extractor.extract()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "tx_id" in df.columns
    assert "customer_blob" in df.columns

def test_postgres_extractor():
    extractor = PostgresExtractor("data/raw/postgres_mock.csv")
    df = extractor.extract()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "id" in df.columns
    assert "sale_date" in df.columns
