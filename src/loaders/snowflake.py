import abc
import json
import logging
import time
import random
from datetime import datetime
from functools import wraps
import pandas as pd
import duckdb

logger = logging.getLogger(__name__)

def retry(exceptions, tries=4, delay=1, backoff=2, jitter=True):
    """
    Decorator for retrying a function with exponential backoff and optional jitter.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(
                        f"Exception encountered: {e} ({e.__class__.__name__}) in {func.__name__}. "
                        f"Retrying in {mdelay:.2f}s... ({mtries - 1} tries left)"
                    )
                    time.sleep(mdelay + (random.uniform(0, 1) if jitter else 0))
                    mdelay *= backoff
                    mtries -= 1
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Dynamically resolve Snowflake exception classes if installed
try:
    import snowflake.connector
    import snowflake.connector.errors as sf_errors
    SF_EXCEPTIONS = (sf_errors.Error,)
except ImportError:
    class DummySnowflakeError(Exception):
        pass
    SF_EXCEPTIONS = (DummySnowflakeError,)



class DBEngine(abc.ABC):
    @abc.abstractmethod
    def execute(self, sql: str):
        """Execute a SQL script containing one or more statements."""
        pass

    @abc.abstractmethod
    def fetch_df(self, query: str) -> pd.DataFrame:
        """Fetch query results as a pandas DataFrame."""
        pass

    @abc.abstractmethod
    def load_df(self, df: pd.DataFrame, table_name: str, schema: str):
        """Bulk load a pandas DataFrame into a staging table."""
        pass


class DuckDBEngine(DBEngine):
    """Local DuckDB engine that mocks Snowflake and registers custom Snowflake functions."""

    def __init__(self):
        logger.info("Initializing local DuckDB connection...")
        try:
            self.conn = duckdb.connect()
            self.conn.execute("ATTACH ':memory:' AS de_integration;")
            self.conn.execute("INSTALL json; LOAD json;")
            
            # Register custom mock Snowflake functions
            self.conn.create_function("try_parse_json", self._try_parse_json, ["VARCHAR"], "VARCHAR", null_handling="special")
            self.conn.create_function("get", self._get, ["VARCHAR", "VARCHAR"], "VARCHAR", null_handling="special")
            self.conn.create_function("try_to_timestamp", self._try_to_timestamp, ["VARCHAR", "VARCHAR"], "TIMESTAMP", null_handling="special")
            logger.info("Local DuckDB engine initialized and mock functions registered successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB connection: {e}", exc_info=True)
            raise

    def _try_parse_json(self, x):
        if not x or pd.isna(x):
            return None
        try:
            json.loads(x)
            return x
        except Exception:
            return None

    def _get(self, obj, key):
        if not obj or not key or pd.isna(obj):
            return None
        try:
            data = json.loads(obj)
            val = data.get(key)
            if val is None:
                return None
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            return str(val)
        except Exception:
            return None

    def _try_to_timestamp(self, val, format_str):
        if not val or pd.isna(val):
            return None
        val_str = str(val).strip()
        fmt_map = {
            'YYYY-MM-DD HH24:MI:SS': '%Y-%m-%d %H:%M:%S',
            'DD/MM/YYYY HH24:MI:SS': '%d/%m/%Y %H:%M:%S',
            'YYYY/MM/DD HH24:MI:SS': '%Y/%m/%d %H:%M:%S',
        }
        py_fmt = fmt_map.get(format_str, '%Y-%m-%d %H:%M:%S')
        try:
            return datetime.strptime(val_str, py_fmt)
        except ValueError:
            return None

    def execute(self, sql: str):
        logger.debug("Executing SQL statements on DuckDB engine.")
        try:
            statements = sql.split(";")
            for stmt in statements:
                cleaned = stmt.strip()
                if cleaned:
                    self.conn.execute(cleaned)
        except Exception as e:
            logger.error(f"DuckDB SQL execution error: {e}", exc_info=True)
            raise

    def fetch_df(self, query: str) -> pd.DataFrame:
        logger.debug(f"Fetching dataframe using query: {query}")
        try:
            return self.conn.execute(query).df()
        except Exception as e:
            logger.error(f"DuckDB DataFrame fetch error for query '{query}': {e}", exc_info=True)
            raise

    def load_df(self, df: pd.DataFrame, table_name: str, schema: str):
        schema_lower = schema.lower()
        table_lower = table_name.lower()
        logger.info(f"Loading {len(df)} rows into DuckDB table: de_integration.{schema_lower}.{table_lower}")
        
        try:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS de_integration.{schema_lower};")
            self.conn.register("df_temp", df)
            self.conn.execute(f"CREATE OR REPLACE TABLE de_integration.{schema_lower}.{table_lower} AS SELECT * FROM df_temp;")
            self.conn.unregister("df_temp")
            logger.info(f"DuckDB table de_integration.{schema_lower}.{table_lower} loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load dataframe to DuckDB table {table_name}: {e}", exc_info=True)
            raise


class SnowflakeEngine(DBEngine):
    """Production Snowflake engine using the snowflake connector."""

    def __init__(self, config: dict):
        self.config = config
        self.conn = self._connect()

    @retry(SF_EXCEPTIONS, tries=4, delay=2, backoff=2)
    def _connect(self):
        logger.info("Attempting connection to Snowflake database...")
        import snowflake.connector
        return snowflake.connector.connect(
            account=self.config["account"],
            user=self.config["user"],
            password=self.config["password"],
            role=self.config.get("role"),
            warehouse=self.config.get("warehouse"),
            database=self.config.get("database", "DE_INTEGRATION"),
            schema=self.config.get("raw_schema", "RAW")
        )

    @retry(SF_EXCEPTIONS, tries=3, delay=1, backoff=2)
    def execute(self, sql: str):
        logger.info("Executing SQL statements on Snowflake...")
        try:
            self.conn.execute_string(sql)
            logger.info("SQL statements executed successfully on Snowflake.")
        except sf_errors.Error as e:
            logger.error(f"Snowflake SQL execution error: {e}", exc_info=True)
            raise

    @retry(SF_EXCEPTIONS, tries=3, delay=1, backoff=2)
    def fetch_df(self, query: str) -> pd.DataFrame:
        logger.info(f"Fetching DataFrame from Snowflake using query: {query}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                df = cursor.fetch_pandas_all()
                logger.info(f"Successfully fetched {len(df)} rows from Snowflake.")
                return df
        except sf_errors.Error as e:
            logger.error(f"Snowflake query fetch error for query '{query}': {e}", exc_info=True)
            raise

    @retry(SF_EXCEPTIONS, tries=3, delay=2, backoff=2)
    def load_df(self, df: pd.DataFrame, table_name: str, schema: str):
        from snowflake.connector.pandas_tools import write_pandas
        
        target_schema = schema.upper()
        target_table = table_name.upper()
        logger.info(f"Bulk loading {len(df)} rows to Snowflake table {target_schema}.{target_table}")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
            
            success, nchunks, nrows, _ = write_pandas(
                self.conn,
                df,
                target_table,
                schema=target_schema,
                auto_create_table=True
            )
            if success:
                logger.info(f"Successfully loaded {nrows} rows in {nchunks} chunks into Snowflake table.")
            else:
                raise RuntimeError(f"Snowflake write_pandas reported failure for table {target_table}.")
        except sf_errors.Error as e:
            logger.error(f"Failed to load data to Snowflake table {target_table}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading dataframe to Snowflake: {e}", exc_info=True)
            raise


def get_engine(engine_type: str, config: dict = None) -> DBEngine:
    """Factory to retrieve DB engine."""
    logger.info(f"Creating database engine of type: {engine_type}")
    if engine_type == "snowflake":
        if not config:
            raise ValueError("Config must be provided for snowflake engine type.")
        return SnowflakeEngine(config)
    elif engine_type == "duckdb":
        return DuckDBEngine()
    else:
        raise ValueError(f"Unknown engine type: {engine_type}")

