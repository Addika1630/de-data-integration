import logging
import time
from src.loaders.snowflake import DBEngine

logger = logging.getLogger(__name__)

class SqlRunner:
    """Helper class to run multi-statement SQL files against the configured DB engine."""

    def __init__(self, engine: DBEngine):
        self.engine = engine

    def run_script(self, script_path: str):
        """Reads and executes SQL from a file path."""
        logger.info(f"Executing SQL script: {script_path}")
        start_time = time.time()
        try:
            with open(script_path, "r", encoding="utf-8") as f:
                sql_script = f.read()
            self.engine.execute(sql_script)
            duration = time.time() - start_time
            logger.info(f"SQL script {script_path} executed successfully in {duration:.3f} seconds.")
        except FileNotFoundError as e:
            logger.error(f"SQL script file not found at '{script_path}': {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to execute SQL script at '{script_path}': {e}", exc_info=True)
            raise

