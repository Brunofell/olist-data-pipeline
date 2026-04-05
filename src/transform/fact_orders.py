import pandas as pd
# Usa aqui de script final

# Um parquet unindo
# - orders
# - order_items
# - payments
# - (opcional) reviews


df_orders = pd.read_csv('data/bronze/olist_orders_dataset.csv')
df_orders_items = pd.read_csv('data/bronze/olist_order_items_dataset.csv')
df_orders_payments = pd.read_csv('data/bronze/olist_order_payments_dataset.csv')
df_orders_reviews = pd.read_csv('data/bronze/olist_order_reviews_dataset.csv')

