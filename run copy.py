from methos import *
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

# Normalize data types
print(df_broker_data.info())
# 1. Data types

df_broker_data['important_score'] = df_broker_data['important_score'].replace("#REF!", int(0))
#df_broker_data['timestamp'] = df_broker_data['timestamp'].apply(datetime)
#df_broker_data['timestamp'] = pd.to_datetime(df_broker_data['timestamp'], unit='s')
df_broker_data = df_broker_data.astype({'important_score': 'int', 'timestamp': 'datetime64[ns]'})
df_broker_data = df_broker_data.convert_dtypes()

print(df_broker_data)
print(df_broker_data.info())
sys.exit()
# 
print(df_brokerchooser.info()) # we could see there is no null value in any field
df_brokerchooser = df_brokerchooser.convert_dtypes()
df_brokerchooser = df_brokerchooser.astype({'created_at': 'datetime64[ns]'})
print(df_brokerchooser.info())
print(df_brokerchooser)

print(df_page_category.info()) # we could see there is no null value in any field
df_page_category = df_page_category.convert_dtypes()
print(df_page_category.info())
print(df_page_category)

# 2. Standardize formats -> there are no numeric types (amounts, measures, anything), easier

#  3. Handling missing values -> método para sacar qué columnas tienen valores nulos
print(df_broker_data.isnull().any())
df_broker_data = df_broker_data.fillna('NULL')
print(df_broker_data)
print(df_broker_data.isnull().any())

print(df_brokerchooser.info())
print(df_brokerchooser.isnull().any())
df_brokerchooser = df_brokerchooser.fillna('NULL')
print(df_brokerchooser)
print(df_brokerchooser.isnull().any())
print(df_brokerchooser.info())

print(df_page_category.info()) # we could see there is no null value in any field
print(df_page_category.isnull().any())



# 4. Possible matches fields in any df
country_residency = df_broker_data['country_residency'].unique()
print(country_residency)
ip_country = df_broker_data['ip_country'].unique()
print(sorted(ip_country))

# Different values from one to another
print(set(country_residency)-set(ip_country))
print(set(ip_country)-set(country_residency))




"""



# Merge datasets on common fields
merged_data = pd.merge(internal_data, broker_data, on='id', how='outer')

# Handle missing data
merged_data.fillna(method='ffill', inplace=True)

# Save the normalized dataset
merged_data.to_csv('normalized_data.csv', index=False)


"""