import pandas as pd


class OrderItemsTransformer:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def convert_dates(self) -> None:

        self.df["shipping_limit_date"] = pd.to_datetime(
            self.df["shipping_limit_date"],
            errors="coerce"
        )

    def run(self) -> pd.DataFrame:

        self.convert_dates()

        return self.df