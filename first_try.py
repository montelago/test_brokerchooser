from methos import *
from datetime import datetime
import sys
import difflib 

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

"""
df_bermudas = df_broker_data[df_broker_data['ip_country']=='Bermuda']
print(df_bermudas)
df_bermudas.to_csv(f'{os.getcwd()}/docs/bermudas.csv', index=False)
df_bermudas = df_brokerchooser[df_brokerchooser['country_name']=='Bermuda']
print(df_bermudas)
df_bermudas.to_csv(f'{os.getcwd()}/docs/bermudas_brokechooser.csv', index=False)
"""
## fuzzy match timestamp + country
merge = exact_matching(df_brokerchooser, df_broker_data)
print(merge)

print(list(merge))
print(merge.shape[0]) # 3.447 con inner solo esas

fuzzy_matching = fuzzy_country_matching(df_brokerchooser, df_broker_data)
print(fuzzy_matching)

apply_fuzzy = apply_fuzzy_matching(df_brokerchooser, df_broker_data)
print(apply_fuzzy)
print(apply_fuzzy.shape[0])


"""
# Define a function for fuzzy matching
def fuzzy_match(row, broker_df, column):
    match = process.extractOne(row[column], broker_df[column], scorer=fuzz.token_sort_ratio)
    return match[0], match[1]  # returns the best match and the score

# Apply fuzzy matching to merge data
conversions['broker_country_name'], conversions['match_score'] = zip(*conversions.apply(fuzzy_match, broker_df=broker_data, column='country_name', axis=1))


# Filter matches with a reasonable threshold
conversions = conversions[conversions['match_score'] >= 80]

# Merge datasets on country_name
merged_data = pd.merge(conversions, broker_data, left_on='broker_country_name', right_on='country_name', how='left')





df_broker_data['timestamp'] = df_broker_data['timestamp'].apply(lambda x: difflib.get_close_matches(x, df_brokerchooser['created_at'])[0])
df_brokerchooser.merge(df_broker_data)

print(df_broker_data.isnull().any())
print(df_brokerchooser.isnull().any())
print(df_page_category.isnull().any())

print(df_broker_data)"""

"""
######## MATCHES DATA
# BROKERCHOOSER

# qué ui elemtns hay?
print(df_brokerchooser['ui_element'].unique()) # 162 ui_elemnts

# solo veo que se pueda mergear entre data from brokerchooser and users' data between timestamp
print("DATA PER FILE")
print(df_page_category.shape[0]) # 40
print(df_broker_data.shape[0]) # 61.840 -> 61.547
print(df_brokerchooser.shape[0]) # 102.441 

merge_data = pd.merge(df_brokerchooser, df_broker_data, left_on='created_at', right_on='timestamp', how='inner')
print(merge_data.shape[0]) # 0 inner 

merge_data = pd.merge(df_brokerchooser, df_broker_data, left_on='created_at', right_on='timestamp', how='left') 

print(df_broker_data['country_residency'].unique()) # 182 ui_elemnts
print(df_broker_data['ip_country'].unique()) # 212
print(df_brokerchooser['country_name'].unique()) # 214

df_df = set(df_broker_data['country_residency'].unique())&set(df_broker_data['ip_country'].unique())
print(len(df_df)) # 159

print(df_broker_data)

#returnMatches(df_brokerchooser['country_name'].unique(),df_broker_data['country_residency'].unique())


# Merging based on nearest timestamp and matching country
df_broker_data['country_name'] = df_broker_data['country_residency']
merged_data = pd.merge_asof(
    df_brokerchooser.sort_values('created_at'),
    df_broker_data.sort_values('timestamp'),
    left_on='created_at',
    right_on='timestamp',
    by=['country_name'],
    direction='nearest'
)
print(merge_data)
"""