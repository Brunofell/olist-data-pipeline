import pandas as pd


class FactOrdersTransformer:

    def __init__(
        self,
        orders_df: pd.DataFrame,
        items_df: pd.DataFrame,
        payments_df: pd.DataFrame,
        reviews_df: pd.DataFrame
    ):

        self.orders_df = orders_df
        self.items_df = items_df
        self.payments_df = payments_df
        self.reviews_df = reviews_df

    def aggregate_payments(self) -> pd.DataFrame:

        return (
            self.payments_df
            .groupby("order_id")
            .agg({
                "payment_value": "sum"
            })
            .reset_index()
        )

    def aggregate_reviews(self) -> pd.DataFrame:

        return (
            self.reviews_df
            .groupby("order_id")
            .agg({
                "review_score": "mean"
            })
            .reset_index()
        )

    def build_fact_table(self) -> pd.DataFrame:

        fact_df = self.items_df.copy()

        fact_df = fact_df.merge(
            self.orders_df[
                [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
            ],
            on="order_id",
            how="left"
        )

        fact_df = fact_df.merge(
            self.aggregate_payments(),
            on="order_id",
            how="left"
        )

        fact_df = fact_df.merge(
            self.aggregate_reviews(),
            on="order_id",
            how="left"
        )

        return fact_df[
            [
                "order_id",
                "product_id",
                "seller_id",
                "customer_id",
                "price",
                "freight_value",
                "payment_value",
                "order_purchase_timestamp",
                "order_delivered_customer_date",
                "review_score"
            ]
        ]

    def run(self) -> pd.DataFrame:

        return self.build_fact_table()