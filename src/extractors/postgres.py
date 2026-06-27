import logging
import pandas as pd
from src.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class PostgresExtractor(BaseExtractor):
    """Concrete extractor for PostgreSQL sales data (mocked via CSV)."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract(self) -> pd.DataFrame:
        """Reads PostgreSQL sales from a local CSV."""
        logger.info(f"Starting extraction from PostgreSQL source path: {self.file_path}")
        try:
            df = pd.read_csv(self.file_path)
            logger.info(f"Successfully extracted {len(df)} rows from PostgreSQL mock source.")
            return df
        except FileNotFoundError as e:
            logger.error(f"PostgreSQL mock source CSV file not found at '{self.file_path}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while extracting PostgreSQL sales: {e}", exc_info=True)
            raise

