import pandas as pd
import glob
import os
import csv
import logging
import datetime
import numpy as np
from fuzzywuzzy import process, fuzz
import sys


def exact_matching(conversions, broker_data):
    merged_data = pd.merge(
        conversions,
        broker_data,
        left_on=['created_at', 'country_name'],
        right_on=['timestamp', 'country_residency'],
        how='inner'
    )
    return merged_data

def fuzzy_country_matching(row, broker_data):
    """Fuzzy match the country_name from conversions with broker_data."""
    country_name = row['country_name']
    best_match = process.extractOne(country_name, broker_data['country_residency'], scorer=fuzz.token_sort_ratio)
    
    # If the best match is above a certain threshold, return the matched row
    if best_match[1] >= 85:  # 85% match threshold
        matched_row = broker_data.loc[broker_data['country_residency'] == best_match[0]]
        return matched_row.iloc[0] if not matched_row.empty else None
    return None

def apply_fuzzy_matching(conversions, broker_data):
    """Apply fuzzy matching for non-exact matches."""
    fuzzy_matches = conversions.apply(fuzzy_country_matching, axis=1, broker_data=broker_data)
    
    # Drop rows that couldn't be matched
    fuzzy_matches.dropna(inplace=True)
    
    # Join fuzzy matched data
    fuzzy_matched_data = pd.merge(conversions, fuzzy_matches, left_index=True, right_index=True, how='inner')
    return fuzzy_matched_data


def get_delimiter(file_path: str) -> str:
    with open(file_path, 'r') as csvfile:
        delimiter = str(csv.Sniffer().sniff(csvfile.read()).delimiter)
        return delimiter

def load_data(file_pattern):
    """Load data from multiple files matching a pattern."""
    files = glob.glob(file_pattern)
    try:
        """Apply method to discover delimiter for each file"""
        df_list = [pd.read_csv(file, header='infer', sep=get_delimiter(file)) for file in files]
        print([df for df in df_list])
    except Exception as e:
        logging.error(f"Error occurred during data load process: {e}")
    return df_list

def deduplicate_records(df, subset):
    """Remove duplicate records based on a subset of columns."""
    return df.drop_duplicates(subset=subset)

def missing_values(df):
    for col in df:
        #get dtype for column
        dt = str(df[col].dtype)
        #check if it is a number
        if dt.__contains__('int') or dt.__contains__('float'):
            df[col] = df[col].fillna(int(0))
        else:
            df[col] = df[col].fillna("").replace("#REF!", "")
    return df


def normalize_data(df):
    """Normalize columns by converting text to lowercase, standardizing dates and other issues"""
    df.columns = [col.lower() for col in df.columns]

    """Normalize columns by converting text to lowercase, standardizing dates and other issues"""
    df = df.convert_dtypes()
    return df


def match_and_merge(data, reference_data, key):
    """Match and merge the main dataset with a reference dataset."""
    return pd.merge(data, reference_data, on=key, how='left')
"""
def save_cleaned_data(df, output_path):
    df.to_csv(output_path, index=False)

def data_processing_pipeline():
    conversions, broker_data, page_category_mapping = load_datasets()
    conversions_normalized = normalize_conversions(conversions, page_category_mapping)
    broker_data_normalized = normalize_broker_data(broker_data)
    unified_data = match_and_merge_datasets(conversions_normalized, broker_data_normalized)
    cleaned_data = handle_missing_values(unified_data)
    save_cleaned_data(cleaned_data, 'cleaned_data/unified_data.csv')

# Run the pipeline
data_processing_pipeline()
"""


def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=2):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()
    
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches'] = m
    
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    
    return df_1