import pandas as pd
import requests
import io
import os
#from google.colab import files

# Mount Google Drive
#drive.mount('/content/drive')


def fetch_cisa_data(url):
    """Fetches the CSV data from the provided GitHub URL."""
    response = requests.get(url)
    if response.status_code == 200:
        return pd.read_csv(io.StringIO(response.text))
    else:
        raise Exception(f"Failed to fetch CSV. HTTP Status: {response.status_code}")

def get_next_icsad_id(local_data):
    """Returns the next ICSAD_ID to start pulling data from."""
    if 'icsad_ID' not in local_data.columns:
        return 0
    return local_data['icsad_ID'].max() + 1

def merge_local_csv_with_cisa_data(local_filepath, cisa_data):
    """Merges local CSV file with data fetched from CISA GitHub CSV."""
    if os.path.exists(local_filepath):
        print(f"File {local_filepath} exists. Reading the file...")
        local_data = pd.read_csv(local_filepath, low_memory=False)
        print("Local data read successfully. Here are the first few rows:")
        print(local_data.head())

        next_icsad_id = get_next_icsad_id(local_data)
        print(f"Next icsad_ID to pull from: {next_icsad_id}")

        new_cisa_data = cisa_data[cisa_data['icsad_ID'] >= next_icsad_id]
        print("New CISA data to be merged:")
        print(new_cisa_data.head())

        new_data = pd.concat([local_data, new_cisa_data]).sort_values(by='icsad_ID').reset_index(drop=True)
    else:
        print(f"File {local_filepath} does not exist. Creating a new file...")
        new_data = cisa_data.sort_values(by='icsad_ID').reset_index(drop=True)

    # Save the updated DataFrame to the same CSV file
    new_data.to_csv(local_filepath, index=False)
    print(f"Updated data saved to '{local_filepath}'.")

if __name__ == '__main__':
    cisa_url = 'https://raw.githubusercontent.com/icsadvprj/ICS-Advisory-Project/main/ICS-CERT_ADV/CISA_ICS_ADV_Master.csv'
    #local_csv_path = '/content/drive/MyDrive/CISA.csv'
    local_csv_path = 'CISA2.csv'
    
    try:
        cisa_csv_data = fetch_cisa_data(cisa_url)
        print("CISA data fetched successfully. Here are the first few rows:")
        print(cisa_csv_data.head())

        merge_local_csv_with_cisa_data(local_csv_path, cisa_csv_data)
    except Exception as e:
        print(str(e))
