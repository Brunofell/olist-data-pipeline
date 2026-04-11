import pandas as pd

df = pd.read_csv('data/bronze/olist_order_items_dataset.csv') 

#  print(df['shipping_limit_date'].head(1))

# print(df.info())

# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 112650 entries, 0 to 112649
# Data columns (total 7 columns):
#  #   Column               Non-Null Count   Dtype  
# ---  ------               --------------   -----  
#  0   order_id             112650 non-null  object 
#  1   order_item_id        112650 non-null  int64
#  2   product_id           112650 non-null  object  
#  3   seller_id            112650 non-null  object 
#  4   shipping_limit_date  112650 non-null  object  !
#  5   price                112650 non-null  float64
#  6   freight_value        112650 non-null  float64
# dtypes: float64(2), int64(1), object(4)
# memory usage: 6.0+ MB
# None

df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')

df.to_parquet('data/silver/items.parquet', index=False)

