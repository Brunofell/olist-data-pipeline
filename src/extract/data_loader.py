import pandas as pd


class DataLoader:

    @staticmethod
    def load_csv(path: str) -> pd.DataFrame:
        """
        Load a CSV file into a pandas DataFrame.
        """
        return pd.read_csv(path)

    @staticmethod
    def load_parquet(path: str) -> pd.DataFrame:
        """
        Load a parquet file into a pandas DataFrame.
        """
        return pd.read_parquet(path)