from src.extract.data_loader import DataLoader
from src.load.parquet_loader import ParquetLoader

from src.transform.orders_transformer import OrdersTransformer
from src.transform.order_items_transformer import OrderItemsTransformer
from src.transform.payments_transformer import PaymentsTransformer
from src.transform.reviews_transformer import ReviewsTransformer
from src.transform.fact_orders_transformer import FactOrdersTransformer

from src.utils.paths import (
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH
)


def main():

    # =========================
    # ORDERS
    # =========================

    orders_df = DataLoader.load_csv(
        BRONZE_PATH / "olist_orders_dataset.csv"
    )

    orders_df = OrdersTransformer(
        orders_df
    ).run()

    ParquetLoader.save(
        orders_df,
        SILVER_PATH / "orders.parquet"
    )

    # =========================
    # ORDER ITEMS
    # =========================

    items_df = DataLoader.load_csv(
        BRONZE_PATH / "olist_order_items_dataset.csv"
    )

    items_df = OrderItemsTransformer(
        items_df
    ).run()

    ParquetLoader.save(
        items_df,
        SILVER_PATH / "items.parquet"
    )

    # =========================
    # PAYMENTS
    # =========================

    payments_df = DataLoader.load_csv(
        BRONZE_PATH / "olist_order_payments_dataset.csv"
    )

    payments_df = PaymentsTransformer(
        payments_df
    ).run()

    ParquetLoader.save(
        payments_df,
        SILVER_PATH / "payments.parquet"
    )

    # =========================
    # REVIEWS
    # =========================

    reviews_df = DataLoader.load_csv(
        BRONZE_PATH / "olist_order_reviews_dataset.csv"
    )

    reviews_df = ReviewsTransformer(
        reviews_df
    ).run()

    ParquetLoader.save(
        reviews_df,
        SILVER_PATH / "reviews.parquet"
    )

    # =========================
    # FACT TABLE
    # =========================

    fact_orders_df = FactOrdersTransformer(
        orders_df,
        items_df,
        payments_df,
        reviews_df
    ).run()

    ParquetLoader.save(
        fact_orders_df,
        GOLD_PATH / "fact_orders.parquet"
    )

    print("Pipeline executed successfully.")


if __name__ == "__main__":
    main()