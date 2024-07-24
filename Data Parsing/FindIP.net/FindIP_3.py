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

def process_csv_chunk(filepath, api_key, start_index, chunk_size, max_queries):
    # Load the CSV file
    data = pd.read_csv(filepath)
    print(f"Processing file: {filepath} from index {start_index} to {start_index + chunk_size}")

    # Initialize new columns if they don't exist
    for column in ['Country Name', 'Latitude', 'Longitude']:
        if column not in data.columns:
            data[column] = None

    end_index = min(start_index + chunk_size, len(data))
    rows_to_process = data.iloc[start_index:end_index]

    query_count = 0

    # Process each row in the current chunk
    for index, row in rows_to_process.iterrows():
        if query_count >= max_queries:
            break
        
        # Skip rows where 'Country Name', 'Latitude', and 'Longitude' are not NaN
        if not (pd.isna(row['Country Name']) and pd.isna(row['Latitude']) and pd.isna(row['Longitude'])):
            continue

        # Skip rows where 'attribute_value' is NaN
        if pd.isna(row['attribute_value']):
            continue
        
        # Extract IP addresses from the 'attribute_value' column
        ip_list = extract_ip_addresses(row['attribute_value'])
        if ip_list:
            print(f"Row {index} - Found IP addresses: {ip_list}")
        
        # Fetch IP info and update the row
        for ip in ip_list:
            if query_count >= max_queries:
                break

            ip_info = fetch_ip_info(ip, api_key)
            query_count += 1

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
    
    # Save the updated data to the CSV file after processing the chunk
    data.to_csv(filepath, index=False)
    print(f"Chunk from index {start_index} to {end_index} saved to {filepath}.")

    return query_count

def find_last_processed_index(data):
    """Finds the index of the last processed row based on 'Country Name' column."""
    last_processed = data['Country Name'].last_valid_index()
    if last_processed is None:
        return 0
    return last_processed + 1

def process_single_csv_file(directory, filename, api_key, chunk_size=1000, max_queries=1000):
    filepath = os.path.join(directory, filename)
    if os.path.exists(filepath):
        data = pd.read_csv(filepath)
        
        # Ensure the necessary columns are present before processing
        for column in ['Country Name', 'Latitude', 'Longitude']:
            if column not in data.columns:
                data[column] = None
        
        start_index = find_last_processed_index(data)
        total_queries = 0

        while start_index < len(data) and total_queries < max_queries:
            queries_processed = process_csv_chunk(filepath, api_key, start_index, chunk_size, max_queries - total_queries)
            total_queries += queries_processed
            start_index += chunk_size
            if total_queries >= max_queries:
                print(f"Reached maximum query limit of {max_queries}. Stopping process.")
                break
    else:
        print(f"File {filename} not found in the directory.")

if __name__ == '__main__':
    directory = '.'
    api_key = 'dade43fa2f184f8e93c999542ca41650'
    filename = 'official_part_11.csv'  # Specify the file to process
    process_single_csv_file(directory, filename, api_key, max_queries=20000)
