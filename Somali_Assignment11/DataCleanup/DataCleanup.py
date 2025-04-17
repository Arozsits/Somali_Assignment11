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


import re
import requests
from decimal import Decimal, ROUND_HALF_UP

class FuelCleaner:
    def __init__(self, input_rows):
        self.rows = input_rows

    def clean(self):
        original_len = len(self.rows)
        seen = set()
        cleaned = []
        anomalies = []

        for row in self.rows:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple in seen:
                continue
            seen.add(row_tuple)

            try:
                price = Decimal(row['Gross Price']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                row['Gross Price'] = format(price, '.2f')
            except:
                continue

            if row.get('Fuel Type', '').strip().lower() == 'pepsi':
                anomalies.append(row)
            else:
                cleaned.append(row)

        self._log_summary(original_len, len(cleaned), len(anomalies), original_len - len(seen))
        return cleaned, anomalies

    def _log_summary(self, original, cleaned, anomalies, dups):
        print("")
        print("CLEANING SUMMARY")
        print("Original rows:      ", original)
        print("Duplicates removed: ", dups)
        print("Cleaned rows:       ", cleaned)
        print("Anomalies found:    ", anomalies)
        print("")

def has_zip(address):
    return bool(re.search(r'\b\d{5}\b', str(address).strip()))

def extract_city_state(address):
    try:
        parts = [p.strip() for p in str(address).split(',') if p.strip()]
        for i, part in enumerate(parts):
            if re.match(r'^[A-Z]{2}(\s+\d{5})?$', part):
                state = part[:2]
                city = parts[i - 1] if i > 0 else None
                return city, state
    except:
        pass
    return None, None

def fill_zip(address, api_key, zip_count, zip_limit):
    if zip_count >= zip_limit or not address or has_zip(address):
        return address, zip_count

    city, state = extract_city_state(address)
    if not city or not state:
        print("Skipping: could not parse city/state")
        return address, zip_count

    try:
        response = requests.get(
            'https://app.zipcodebase.com/api/v1/search',
            params={
                'apikey': api_key,
                'city': city,
                'state': state,
                'country': 'US'
            },
            timeout=5  # Prevent hanging
        )
        if response.status_code == 200:
            data = response.json()
            zips = data.get('results', {}).get(city, [])
            if zips:
                zip_code = zips[0]
                print(f"ZIP added: {zip_code} to {city}, {state}")
                return address.strip() + " " + zip_code, zip_count + 1
    except Exception as e:
        print(f"API error for {city}, {state}: {e}")

    return address, zip_count

