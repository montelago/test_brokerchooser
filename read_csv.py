import pandas as pd


print("Read CSV")

print("PAGE CATEGORY MAPPING")
df_page = pd.read_csv('page_category_mapping.csv', sep=';' ,header='infer')
print(df_page)

print("BROKER DATA")
# It has several NaN in country_residency column
df_data = pd.read_csv('broker_data.csv', header='infer')
print(df_data)

print("BROKERCHOOSER CONVERSIONS")
df_conversions = pd.read_csv('brokerchooser_conversions.csv', sep = ';', header='infer' , on_bad_lines='warn')
print(df_conversions)

print("DATA PER FILE")
print(df_page.shape[0]) # 40
print(df_data.shape[0]) # 61.840
print(df_conversions.shape[0]) # 102.441

import os
current_directory = os.getcwd()
print(current_directory) 
df_conversions_proof = pd.read_csv(f'{current_directory}/docs/batch_brokerchooser_conversions.csv',\
                                    sep = ';', header='infer' , on_bad_lines='warn')
print(df_conversions_proof)