import pandas as pd

df = pd.read_csv('data/bronze/olist_orders_dataset.csv')

#   INFOS DAS COLUNAS

# Index(['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
#        'order_approved_at', 'order_delivered_carrier_date',
#        'order_delivered_customer_date', 'order_estimated_delivery_date'],
#       dtype='object')


# order_id                         object
# customer_id                      object
# order_status                     object
# order_purchase_timestamp         object
# order_approved_at                object
# order_delivered_carrier_date     object
# order_delivered_customer_date    object
# order_estimated_delivery_date    object
# dtype: object


# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 99441 entries, 0 to 99440
# Data columns (total 8 columns):
#  #   Column                         Non-Null Count  Dtype 
# ---  ------                         --------------  ----- 
#  0   order_id                       99441 non-null  object
#  1   customer_id                    99441 non-null  object
#  2   order_status                   99441 non-null  object
#  3   order_purchase_timestamp       99441 non-null  object
#  4   order_approved_at              99281 non-null  object
#  5   order_delivered_carrier_date   97658 non-null  object
#  6   order_delivered_customer_date  96476 non-null  object
#  7   order_estimated_delivery_date  99441 non-null  object
# dtypes: object(8)
# memory usage: 6.1+ MB
# None


# print(df.apply(lambda col: col.dropna().iloc[0]))

# order_id                         e481f51cbdc54678b7cc49136f2d6af7
# customer_id                      9ef432eb6251297304e76186b10a928d
# order_status                                            delivered
# order_purchase_timestamp                      2017-10-02 10:56:33
# order_approved_at                             2017-10-02 11:07:15
# order_delivered_carrier_date                  2017-10-04 19:55:00
# order_delivered_customer_date                 2017-10-10 21:25:13
# order_estimated_delivery_date                 2017-10-18 00:00:00
# dtype: object

# Convertendo datas...

df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')
df['order_approved_at'] = pd.to_datetime(df['order_approved_at'], errors='coerce')
df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'], errors='coerce')
df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')
df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'], errors='coerce')

# order_id                                 object
# customer_id                              object
# order_status                             object
# order_purchase_timestamp         datetime64[ns]
# order_approved_at                datetime64[ns]
# order_delivered_carrier_date     datetime64[ns]
# order_delivered_customer_date    datetime64[ns]
# order_estimated_delivery_date    datetime64[ns]

# DUPLICADOS

duplicados = df[df.duplicated(keep='first')] # da empty então acho que nçao tem duplicatas
# duplicados = df[df.duplicated(subset=['order_id'])] # o order_id deve ser único

## print(duplicados)

df.drop_duplicates(keep='first', inplace=True)

# NORMALIZAR

df['order_status'] = df['order_status'].str.lower().str.strip()

#print(df.info())
# print(df.isna().sum()) # verifical nulos, tem nulos mas nessa tabela isso pe importante

df.to_parquet('data/silver/orders.parquet', index=False)
# index=False → não salva o índice (boa prática)