import pandas as pd
import glob
import os
import csv
import logging
import datetime
import numpy as np
import sys
from rapidfuzz import process, fuzz
import schedule
import time 

def get_delimiter(file_path: str) -> str:
    """Obtain delimiter from a csv file."""
    with open(file_path, 'r') as csvfile:
        delimiter = str(csv.Sniffer().sniff(csvfile.read()).delimiter)
        return delimiter

def load_data(file_pattern):
    """Load data from multiple files."""
    files = glob.glob(file_pattern)
    try:
        """Save each read csv in dfs list."""
        df_list = [pd.read_csv(file, header='infer', sep=get_delimiter(file)) for file in files]
    except Exception as e:
        logging.error(f"Error occurred during data load process: {e}")
    return df_list


def deduplicate_records(df, subset):
    """Remove duplicate records based on a subset of columns."""
    return df.drop_duplicates(subset=subset)

def missing_values(df):
    """Handle missing values depend on column type."""
    for col in df:
        dt = str(df[col].dtype)
        if dt.__contains__('int') or dt.__contains__('float'):
            df[col] = df[col].fillna(int(0))
        else:
            df[col] = df[col].fillna("").replace("#REF!", "")
    return df


def normalize_data(df):
    """Convert text to lowercase."""
    df.columns = [col.lower() for col in df.columns]

    """Standarize dates format and int column"""
    df_columns = list(df)
    if ['timestamp','important_score'] in df_columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        #df['timestamp'] = df['timestamp'].apply(datetime.fromtimestamp)
        df['important_score'] = df['important_score'].replace("#REF!", int(0))
        df = df.astype({'important_score': 'int'}) # , 'timestamp': 'datetime64[ns]'
    elif 'created_at' in df_columns:
        df = df.astype({'created_at': 'datetime64[ns]'})

    print(df.info())    
    """Convert rest of the types not managed."""
    df = df.convert_dtypes()

    if 'timestamp' in list(df):
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

    """Rename columns for having same names in each dataframe. Errors by default are ignored."""
    df = df.rename({'created_at': 'timestamp', 'ip_country': 'country_name'}, axis='columns')

    print(df.info())
    return df

def page_category_merge(brokerchooser, page_category):
    df = pd.merge(
        brokerchooser,
        page_category,
        on= ['measurement_category'],
        how='left'
    )
    return df

def exact_matching(brokerchooser, broker_data):
    """Perform merge data between brokerchooser data and users' data."""
    merged_data = pd.merge(
        brokerchooser,
        broker_data,
        on= ['timestamp', 'country_name'],
        how='inner'
    )
    return merged_data

def fuzzy_matching(row, df_to_match, columnA, columnB):
    """Combine the two columns into one string."""
    string_to_match = f"{row[columnA]} {row[columnB]}"
    
    """Iterate over the df_to_match and find the best match."""
    best_match = None
    highest_score = 0
    
    for _, row_to_match in df_to_match.iterrows():
        string_to_compare = f"{row_to_match[columnA]} {row_to_match[columnB]}"
        score = fuzz.token_sort_ratio(string_to_match, string_to_compare)
        
        if score > highest_score:
            highest_score = score
            best_match = row_to_match
    
    return pd.Series([best_match[columnA], best_match[columnB], highest_score])

def apply_fuzzy_matching(brokerchooser, broker_data):
    brokerchooser[['MatchedA', 'MatchedB', 'match_score']] = \
    brokerchooser.apply(fuzzy_matching, df_to_match=broker_data, columnA='timestamp', columnB='country_name', axis=1)
    brokerchooser.dropna(inplace=True)
    brokerchooser = pd.merge(brokerchooser, fuzzy_matching, on = ['timestamp','country_name'], how='inner')
    return brokerchooser

def combine_data(exact_matching, apply_fuzzy_matching):
    combined_data = pd.concat([exact_matching, apply_fuzzy_matching]).drop_duplicates()
    return combined_data

def data_processing_pipeline():
    data = load_data(f'{os.getcwd()}/docs/batch_*.csv')

    df_brokerchooser = data[0]
    df_broker_data = data[1]
    df_page_category = data[2]

    df_brokerchooser = deduplicate_records(df_brokerchooser, list(df_brokerchooser))
    df_brokerchooser = normalize_data(df_brokerchooser)
    df_brokerchooser = missing_values(df_brokerchooser)

    df_broker_data = deduplicate_records(df_broker_data, list(df_broker_data))
    df_broker_data = normalize_data(df_broker_data)
    df_broker_data = missing_values(df_broker_data)

    df_page_category = deduplicate_records(df_page_category, list(df_page_category))
    df_page_category = normalize_data(df_page_category)
    df_page_category = missing_values(df_page_category)

    """Match dfs."""
    df_brokerchooser = page_category_merge(df_brokerchooser, df_page_category)
    exact_maching = exact_matching(df_brokerchooser, df_broker_data)
    fuzzy_matching = apply_fuzzy_matching(df_brokerchooser, df_broker_data)
    print(fuzzy_matching.shape[0])
    print(exact_maching.shape[0])
    merged_and_match = combine_data(exact_matching, fuzzy_matching)

    merged_and_match.to_csv('BrokerChooser.csv', index=False)
    
# Run the pipeline
data_processing_pipeline()



