import pandas as pd


class ParquetLoader:

    @staticmethod
    def save(df: pd.DataFrame, path: str) -> None:
        """
        Save a DataFrame as parquet.
        """
        df.to_parquet(path, index=False)