# File Name : Somali_Assignment11
# Student Name: Andrew Rozsits, Liam Vasey
# email:  rozsitaj@mail.uc.edu, vaseylh@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   04/17/2025
# Course #/Section:   IS 4010-001
# Semester/Year:   Spring 2025
# Brief Description of the assignment: This assignment involves building a data cleaning tool that processes a CSV file by removing duplicates and anomalies. While also correcting data using an API.

# Brief Description of what this module does. This module shows us how to clean data using python and external APIs
# Citations: chatgpt.com



import csv
import os
import pandas as pd
from DataCleanup.DataCleanup import FuelCleaner, fill_zip, has_zip

def load_data(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        return list(csv.DictReader(file))

def save_data(file_path, fieldnames, rows):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    input_file = 'Data/fuelPurchaseData.csv'
    cleaned_file = 'Data/cleanedData.csv'
    anomalies_file = 'Data/dataAnomalies.csv'
    api_key = '9410c3f0-162b-11f0-88fc-15534b19f2e1'

    print("Loading data...")
    data = load_data(input_file)

    print("Cleaning data...")
    cleaner = FuelCleaner(data)
    cleaned, anomalies = cleaner.clean()

    print("Saving cleaned and anomaly data...")
    if cleaned:
        save_data(cleaned_file, cleaned[0].keys(), cleaned)
    if anomalies:
        save_data(anomalies_file, anomalies[0].keys(), anomalies)

    print("Applying ZIP code lookups to the first 5 missing addresses...")
    df = pd.read_csv(cleaned_file, low_memory=False)

    zip_limit = 5
    zip_count = 0
    updated_addresses = []

    for addr in df['Full Address']:
        if not has_zip(addr) and zip_count < zip_limit:
            updated, zip_count = fill_zip(addr, api_key, zip_count, zip_limit)
            updated_addresses.append(updated)
        else:
            updated_addresses.append(addr)

    df['Full Address'] = updated_addresses
    df.to_csv(cleaned_file, index=False)

    print("Done. Files saved:")
    print("- Cleaned data:    ", cleaned_file)
    print("- Anomalies file:  ", anomalies_file)


    for i, addr in enumerate(df['Full Address']):
        if not has_zip(addr) and zip_count < zip_limit:
            updated, zip_count = fill_zip(addr, api_key, zip_count, zip_limit)
            updated_addresses.append(updated)
            print(f"Processed row {i+1}, ZIPs used: {zip_count}")
        else:
            updated_addresses.append(addr)
if __name__ == '__main__':
    main()




    #  api_key = '9410c3f0-162b-11f0-88fc-15534b19f2e1'   Other