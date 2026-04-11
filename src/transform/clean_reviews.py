import pandas as pd

df = pd.read_csv('data/bronze/olist_order_reviews_dataset.csv')

# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 99224 entries, 0 to 99223
# Data columns (total 7 columns):
#  #   Column                   Non-Null Count  Dtype 
# ---  ------                   --------------  ----- 
#  0   review_id                99224 non-null  object
#  1   order_id                 99224 non-null  object
#  2   review_score             99224 non-null  int64 / 1 - 5
#  3   review_comment_title     11568 non-null  object // NaN || recomendo
#  4   review_comment_message   40977 non-null  object // texto || Nan
#  5   review_creation_date     99224 non-null  object // 2017-06-23 00:00:00
#  6   review_answer_timestamp  99224 non-null  object // 2018-01-20 21:25:45
# dtypes: int64(1), object(6)
# memory usage: 5.3+ MB

df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')

# Normalizar

df['review_comment_title'] = df['review_comment_title'].str.strip()
df['review_comment_message'] = df['review_comment_message'].str.strip()

df['review_comment_title'] = df['review_comment_title'].replace('', pd.NA)
df['review_comment_message'] = df['review_comment_message'].replace('', pd.NA)

df['has_comment'] = df['review_comment_message'].notna()

# Validação de score

assert df['review_score'].between(1, 5).all() # true

df.to_parquet('data/silver/reviews.parquet', index=False)