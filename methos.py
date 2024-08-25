import pandas as pd
import glob
import os
import csv
import logging
import datetime
import numpy as np

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

def normalize_data(df):
    """Normalize columns by converting text to lowercase and standardizing dates."""
    df.columns = [col.lower() for col in df.columns]

    datetime.fromtimestamp(timestamp)
    df = df['timestamp'].apply(datetime.fromtimestamp)

    #df['name'] = df['name'].str.lower()
    #df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df


def match_and_merge(data, reference_data, key):
    """Match and merge the main dataset with a reference dataset."""
    return pd.merge(data, reference_data, on=key, how='left')

