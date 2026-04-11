import pandas as pd

df = pd.read_csv('data/bronze/olist_order_payments_dataset.csv')

# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 103886 entries, 0 to 103885
# Data columns (total 5 columns):
#  #   Column                Non-Null Count   Dtype  
# ---  ------                --------------   -----  
#  0   order_id              103886 non-null  object 
#  1   payment_sequential    103886 non-null  int64  // 1
#  2   payment_type          103886 non-null  object // credit_card
#  3   payment_installments  103886 non-null  int64   // 8
#  4   payment_value         103886 non-null  float64  // 9.6
# dtypes: float64(1), int64(2), object(2)
# memory usage: 4.0+ MB

df['payment_type'] = df['payment_type'].str.lower().str.strip()

df.to_parquet('data/silver/payments.parquet', index=False)