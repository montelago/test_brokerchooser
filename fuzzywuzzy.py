from methos_2 import *
from datetime import datetime
import sys


# Example usage
data = load_data(f'{os.getcwd()}/docs/batch_*.csv')
print(type(data))

# Deduplicates columns
data = [deduplicate_records(df, list(df)) for df in data]

df_brokerchooser = data[0]
df_broker_data = data[1]
df_page_category = data[2]

print(df_broker_data.sort_values(['timestamp']))
print(df_brokerchooser.sort_values(['created_at']))


# 2. Standardize formats -> there are no numeric types (amounts, measures, anything), easier

# Normalize data types
print(df_broker_data.info())

df_broker_data['important_score'] = df_broker_data['important_score'].replace("#REF!", int(0))
df_broker_data['timestamp'] = pd.to_datetime(df_broker_data['timestamp'], unit='s')
df_broker_data = df_broker_data.astype({'important_score': 'int', 'timestamp': 'datetime64[ns]'})
df_brokerchooser = df_brokerchooser.astype({'created_at': 'datetime64[ns]'})

df_broker_data = normalize_data(df_broker_data)
df_brokerchooser = normalize_data(df_brokerchooser)
df_page_category = normalize_data(df_page_category)
print(df_broker_data)

#  3. Handling missing values -> método para sacar qué columnas tienen valores nulos
df_broker_data = missing_values(df_broker_data)
df_brokerchooser = missing_values(df_brokerchooser)
df_page_category = missing_values(df_page_category)


# Apply fuzzy matching
fuzzy_matched_data = apply_fuzzy_matching(df_brokerchooser, df_broker_data)

print(df_brokerchooser)
