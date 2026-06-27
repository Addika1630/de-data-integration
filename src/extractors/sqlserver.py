import logging
import pandas as pd
from src.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class SqlServerExtractor(BaseExtractor):
    """Concrete extractor for SQL Server sales data (mocked via CSV)."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract(self) -> pd.DataFrame:
        """Reads SQL Server sales from a local CSV."""
        logger.info(f"Starting extraction from SQL Server source path: {self.file_path}")
        try:
            df = pd.read_csv(self.file_path)
            logger.info(f"Successfully extracted {len(df)} rows from SQL Server mock source.")
            return df
        except FileNotFoundError as e:
            logger.error(f"SQL Server mock source CSV file not found at '{self.file_path}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while extracting SQL Server sales: {e}", exc_info=True)
            raise

