import pandas as pd
import glob
import os
import csv
import logging
import datetime
import numpy as np
import sys
from rapidfuzz import process, fuzz
from data_processing_pipeline import *


def data_processing_users():
    data = load_data(f'{os.getcwd()}/docs/batch_*.csv')

    df_broker_data = data[1]
    df_page_category = data[2]

    df_broker_data = deduplicate_records(df_broker_data, list(df_broker_data))
    df_broker_data = normalize_data(df_broker_data)
    df_broker_data = missing_values(df_broker_data)
    df_broker_data['important_score'] = pd.to_numeric(df_broker_data['important_score'], errors='coerce')
    print(df_broker_data.info())

    df_page_category = deduplicate_records(df_page_category, list(df_page_category))
    df_page_category = normalize_data(df_page_category)
    df_page_category = missing_values(df_page_category)

    """Match dfs."""
    df_brokerchooser = page_category_merge(df_brokerchooser, df_page_category)
    exact_maching = exact_matching(df_brokerchooser, df_broker_data)
    return exact_maching
    
# Run the data
exact_maching = data_processing_users()

# 1. Number of rows per country_name and mobile connection
country_name_count = exact_maching.groupby(['country_name','is_mobile']).size().sort_values()

# Top 10 countries with more connections made.
""" As we can see, then main countries with connections are: 
    We can also view there is some countries dont have mobile connections or viceversa."""
country_name_top_10 =country_name_count.tail(20)
print(country_name_top_10)


# 2. Is there any significant different between mobile or not mobile connections?
"""There is no difference, it's more and less the same percentage."""
mobile_count = exact_maching.groupby('is_mobile').size()
print(mobile_count)


# 3. Important score - which countries are the ones who host the better clients?
"""First of all we analyze values from important score. It has interval from 0 to 99."""
important_score_count = exact_maching.groupby('important_score').size().sort_values()
print(important_score_count)
print(exact_maching['important_score'].max())
print(exact_maching['important_score'].sort_values().tail(40))

"""We can observe there are many countries with just one high value client. So, whar is the mean of this metric?
    Are the values/clients concentrated near 0? Moreover, are the most of the clients low value?
    As we can see, mean value is almost 0. And the 75% of the data ditributes around low values."""
media = exact_maching['important_score'].mean()
print(media) # 0.797634377831831

# Calcular los cuartiles
primer_cuartil = exact_maching['important_score'].quantile(0.25)
mediana = exact_maching['important_score'].quantile(0.50)  # También se puede calcular con median()
tercer_cuartil = exact_maching['important_score'].quantile(0.75)

print(f"Primer cuartil (25%): {primer_cuartil}")
print(f"Mediana (50%): {mediana}")
print(f"Tercer cuartil (75%): {tercer_cuartil}")

"""If we filter data of important score more than 20, we see there are unsignificant quantity."""
important_score_country = exact_maching[exact_maching['important_score']>20]
print(important_score_country.shape[0])

"""We could try also to establish some relation between measurement_category/page_category and clients."""