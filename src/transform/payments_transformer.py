import pandas as pd


class PaymentsTransformer:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def normalize_payment_type(self) -> None:

        self.df["payment_type"] = (
            self.df["payment_type"]
            .str.lower()
            .str.strip()
        )

    def run(self) -> pd.DataFrame:

        self.normalize_payment_type()

        return self.df