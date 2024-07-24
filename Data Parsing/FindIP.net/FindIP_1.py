import pandas as pd
import requests
import re
import os

def extract_ip_addresses(text):
    """Extracts IP addresses from given text using a regex."""
    ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    return re.findall(ip_pattern, text)

def fetch_ip_info(ip_address, api_key):
    """Fetches IP information from the API."""
    url = f'https://api.findip.net/{ip_address}/?token={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error querying API for {ip_address}: {str(e)}")
        return None

def process_csv(filepath, api_key):
    # Load the CSV file
    data = pd.read_csv(filepath)
    print(f"Processing file: {filepath}")
    
    # Initialize new columns if they don't exist
    if 'Country Name' not in data.columns:
        data['Country Name'] = None
    if 'Latitude' not in data.columns:
        data['Latitude'] = None
    if 'Longitude' not in data.columns:
        data['Longitude'] = None
    
    # Process each row to find IP addresses and fetch their info
    for index, row in data.iterrows():
        # Only process rows where Country Name, Latitude, and Longitude are NaN
        if pd.isna(row['Country Name']) or pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
            if pd.isna(row['attribute_value']):
                continue
            ip_list = extract_ip_addresses(row['attribute_value'])
            if ip_list:
                print(f"Row {index} - Found IP addresses: {ip_list}")
            for ip in ip_list:
                ip_info = fetch_ip_info(ip, api_key)
                if ip_info:
                    country = ip_info.get('country', {}).get('names', {}).get('en')
                    latitude = ip_info.get('location', {}).get('latitude')
                    longitude = ip_info.get('location', {}).get('longitude')
                    
                    if country and latitude and longitude:
                        data.at[index, 'Country Name'] = country
                        data.at[index, 'Latitude'] = latitude
                        data.at[index, 'Longitude'] = longitude
                        print(f"Saved Country Name: {country}, Latitude: {latitude}, Longitude: {longitude} into row {index}")
                    else:
                        print(f"Warning: Incomplete data for IP {ip}.")
    
    # Save the updated data to the same CSV file
    data.to_csv(filepath, index=False)
    print(f"Updated CSV saved to {filepath}.")

def process_multiple_csv_files(directory, api_key):
    i = 1
    while True:
        filename = f'official_part_{i}.csv'
        filepath = os.path.join(directory, filename)
        if os.path.exists(filepath):
            process_csv(filepath, api_key)
            i += 1
        else:
            print(f"No more files found after {filename}.")
            break

if __name__ == '__main__':
    directory = '.'
    api_key = 'dade43fa2f184f8e93c999542ca41650'
    process_multiple_csv_files(directory, api_key)
