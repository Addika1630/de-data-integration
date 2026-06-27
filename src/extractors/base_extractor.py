from abc import ABC, abstractmethod
import pandas as pd

class BaseExtractor(ABC):
    """Abstract base class representing the data extraction contract."""

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extracts data from the source and returns it as a pandas DataFrame."""
        pass
