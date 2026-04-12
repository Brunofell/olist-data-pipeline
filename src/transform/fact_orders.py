import pandas as pd
# Usa aqui de script final

df_orders = pd.read_parquet('data/silver/orders.parquet')
df_items = pd.read_parquet('data/silver/items.parquet')
df_payments = pd.read_parquet('data/silver/payments.parquet')
df_reviews = pd.read_parquet('data/silver/reviews.parquet')


df_fact = df_items.copy() # a base seria todos os items de pedido

# print(df_fact.shape) # (112650, 7)

# merge com orders (items + orders)
df_fact = df_fact.merge(
    df_orders[[
    'order_id',
    'customer_id',
    'order_status',
    'order_purchase_timestamp',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
    ]],
    on='order_id',
    how='left'
)

# print(df_fact.shape) # (112650, 12)

# # Payments: Vamos fazer agregação para evitar a duplicação de registros, e calcular a soma

df_payments_agg =  df_payments.groupby('order_id').agg({
    'payment_value': 'sum'
}).reset_index()

df_fact = df_fact.merge(
    df_payments_agg,
    on='order_id',
    how='left'
)

# print(df_fact.shape) # (112650, 13)

# # Reviews, vamos fazer o agg também para evitar duplicação de registro e calcular a média
df_reviews_agg = df_reviews.groupby('order_id').agg({
    'review_score': 'mean'
}).reset_index()

df_fact = df_fact.merge(
    df_reviews_agg,
    on='order_id',
    how='left'
)

#print(df_fact.shape) # (112650, 14)

df_fact = df_fact[[
    'order_id',
    'product_id',
    'seller_id',
    'customer_id',

    'price',
    'freight_value',
    'payment_value',

    'order_purchase_timestamp',
    'order_delivered_customer_date',

    'review_score'
]]

df_fact.to_parquet('data/gold/fact_orders.parquet', index=False)