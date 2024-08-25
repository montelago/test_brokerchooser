import pandas as pd
import glob
import os
import csv
import logging
import datetime
import numpy as np
import sys
from rapidfuzz import process, fuzz

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


def exact_matching(conversions, broker_data):
    merged_data = pd.merge(
        conversions,
        broker_data,
        left_on=['created_at', 'country_name'],
        right_on=['timestamp', 'country_residency'],
        how='inner'
    )
    return merged_data


def fuzzy_country_matching(brokechooser, broker_data):
    """Fuzzy match the country_name from conversions with broker_data."""
    best_match = process.extractOne(brokechooser['country_name'], broker_data['country_residency'], scorer=fuzz.token_sort_ratio)
    #print(best_match)
    
    # If the best match is above a certain threshold, return the matched row
    if best_match[1] >= 85:  # 85% match threshold
        matched_row = broker_data.loc[broker_data['country_residency'] == best_match[0]]
        return matched_row.iloc[0] if not matched_row.empty else None
    return None

def apply_fuzzy_matching(conversions, broker_data):
    fuzzy_matches = conversions.apply(fuzzy_country_matching, axis=1, broker_data=broker_data)
    fuzzy_matches.dropna(inplace=True)
    fuzzy_matched_data = pd.merge(conversions, fuzzy_matches, left_index=True, right_index=True, how='inner')
    return fuzzy_matched_data


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

