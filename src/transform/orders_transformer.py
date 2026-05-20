import pandas as pd


class OrdersTransformer:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def convert_dates(self) -> None:

        date_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]

        for column in date_columns:
            self.df[column] = pd.to_datetime(
                self.df[column],
                errors="coerce"
            )

    def remove_duplicates(self) -> None:
        self.df.drop_duplicates(inplace=True)

    def normalize_text(self) -> None:
        self.df["order_status"] = (
            self.df["order_status"]
            .str.lower()
            .str.strip()
        )

    def run(self) -> pd.DataFrame:
        """
        Execute all transformation steps.
        """

        self.convert_dates()
        self.remove_duplicates()
        self.normalize_text()

        return self.df