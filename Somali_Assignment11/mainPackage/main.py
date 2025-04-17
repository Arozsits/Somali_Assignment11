# main.py

import csv
import os
import pandas as pd
from DataCleanup.DataCleanup import FuelCleaner, fill_zip, has_zip

def load_data(file_path):
    """
    Loads CSV data into a list of dictionaries.

    @param file_path: Path to the input CSV file
    @returns: List of row dictionaries
    """
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        return list(csv.DictReader(file))

def save_data(file_path, fieldnames, rows):
    """
    Saves a list of dictionaries to a CSV file.

    @param file_path: Destination path
    @param fieldnames: List of column names
    @param rows: List of data dictionaries
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    """
    Main entry point for data cleaning:
    - Loads raw CSV
    - Cleans data (deduplication, price fix, anomaly split)
    - Performs ZIP code lookup on first 5 addresses missing ZIPs
    - Saves cleaned and anomaly data
    """
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

    print("Applying ZIP code lookups to first 5 missing ZIPs...")
    df = pd.read_csv(cleaned_file, low_memory=False)

    zip_limit = 0
    zip_count = 0

    for i in range(len(df)):
        if zip_count >= zip_limit:
            break
        address = df.at[i, 'Full Address']
        if not has_zip(address):
            updated, zip_count = fill_zip(address, api_key, zip_count, zip_limit)
            df.at[i, 'Full Address'] = updated
            print(f"Row {i+1}: ZIP added (Total so far: {zip_count})")

    df.to_csv(cleaned_file, index=False)

    print("Done. Files saved:")
    print("- Cleaned data:    ", cleaned_file)
    print("- Anomalies file:  ", anomalies_file)

if __name__ == '__main__':
    main()





 