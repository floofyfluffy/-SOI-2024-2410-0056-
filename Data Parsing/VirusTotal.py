import pandas as pd
import os
import requests
import time

# Function to call the VirusTotal API and extract tactics, techniques, and signatures for a given hash value
def query_virustotal(hash_value, api_key):
    url = f"https://www.virustotal.com/api/v3/files/{hash_value}/behaviour_mitre_trees"
    headers = {
        "accept": "application/json",
        "x-apikey": api_key
    }

    response = requests.get(url, headers=headers)
    print(f"\n=== VirusTotal API Response for Hash: {hash_value} ===")
    print(f"Status Code: {response.status_code}")
    data = response.json()
    print(f"Response Data: {data}\n")

    if response.status_code == 429:
        print("Error: Quota exceeded, stopping further requests.")
        return "quota_exceeded"
    if response.status_code == 404:
        print(f"Error: No data found for {hash_value}. Status Code: 404")
        return {}
    elif response.status_code != 200:
        print(f"Error: Failed to retrieve data for {hash_value}. Status Code: {response.status_code}")
        return {}

    if 'data' in data and 'CAPA' in data['data']:
        capa_data = data['data']['CAPA']
        return capa_data
    else:
        print(f"Warning: CAPA data not found for {hash_value}.")
        return {}

# Load the CSV file and process the data
def process_csv(input_file_path, api_key):
    df = pd.read_csv(input_file_path, low_memory=False)

    for col in ['Tactic_Info', 'Technique_Info', 'Signature_Info']:
        if col not in df.columns:
            df[col] = None

    hash_types = ['sha256', 'sha1', 'md5']

    last_value_row = df['Tactic_Info'].last_valid_index()
    start_index = 0 if last_value_row is None else last_value_row + 1

    for index, row in df.iloc[start_index:].iterrows():
        if pd.isna(row['Tactic_Info']) and pd.isna(row['Technique_Info']) and pd.isna(row['Signature_Info']):
            attribute_value = row['attribute_value']
            if pd.isna(attribute_value):
                continue

            attribute_type = row['attribute_type'].lower()
            if any(ht in attribute_type for ht in hash_types):
                hash_value = str(attribute_value).strip()
                print(f"Processing row {index} with hash value: {hash_value}")

                capa_data = query_virustotal(hash_value, api_key)

                if capa_data == "quota_exceeded":
                    return "quota_exceeded"

                if not capa_data:
                    print(f"No relevant data found for hash value: {hash_value}")
                    continue

                tactics = capa_data.get('tactics', [])
                tactic_info = []
                technique_info = []
                signature_info = []

                for tactic in tactics:
                    tactic_info.append(f"{tactic['id']}: {tactic['name']}")
                    techniques = tactic.get('techniques', [])
                    for technique in techniques:
                        technique_info.append(f"{technique['id']}: {technique['name']}")
                        signatures = technique.get('signatures', [])
                        for signature in signatures:
                            signature_info.append(f"{signature['severity']}: {signature['description']}")

                tactic_str = '; '.join(tactic_info)
                technique_str = '; '.join(technique_info)
                signature_str = '; '.join(signature_info)

                df.at[index, 'Tactic_Info'] = tactic_str
                df.at[index, 'Technique_Info'] = technique_str
                df.at[index, 'Signature_Info'] = signature_str

                time.sleep(15)  # Wait to respect the rate limit

    df.to_csv(input_file_path, index=False)
    print(f"Updated CSV saved to {input_file_path}")

    return None

# Function to process multiple CSV files
def process_multiple_csv_files(file_paths, api_key):
    for file_path in sorted(file_paths):
        print(f"Processing file: {file_path}")
        quota_exceeded = process_csv(file_path, api_key)
        if quota_exceeded == "quota_exceeded":
            print(f"Quota exceeded while processing {file_path}. Stopping further requests.")
            break
        print(f"Finished processing file: {file_path}\n")

# Variables
api_key = ''
input_directory = '.'
file_pattern = 'official_part_*.csv'

# Find all files matching the pattern in the directory
file_paths = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.startswith('official_part_') and f.endswith('.csv')]

# Run the function
process_multiple_csv_files(file_paths, api_key)
