import pandas as pd
import os

def load_data(file_path, required_columns, drop_column=None):
    """Loads CSV data from a given file path, ensuring all required columns are present, and optionally dropping a specified column."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No file found at {file_path}")
    try:
        data = pd.read_csv(file_path, encoding='utf-8', low_memory=False, on_bad_lines='warn')
        data = data.loc[:, ~data.columns.duplicated()]  # Remove duplicate columns if present
        for column in required_columns:
            if column not in data.columns:
                data[column] = pd.NA
        if drop_column and drop_column in data.columns:
            data.drop(columns=[drop_column], inplace=True)
        return data
    except pd.errors.ParserError as e:
        print(f"CSV parsing error in {file_path}: {e}")
        raise

def save_data(data, file_path):
    data.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Data saved to {file_path}. Total rows: {len(data)}")

def append_data(file_path, secondary_file_path, required_columns, drop_column=None):
    primary_data = load_data(file_path, required_columns, drop_column=drop_column)
    secondary_data = load_data(secondary_file_path, required_columns, drop_column=drop_column)

    print(f"Appending to {file_path}...")

    # Ensure icsad_ID is numeric
    primary_data['icsad_ID'] = pd.to_numeric(primary_data['icsad_ID'], errors='coerce')
    secondary_data['icsad_ID'] = pd.to_numeric(secondary_data['icsad_ID'], errors='coerce')

    print(f"Primary data shape: {primary_data.shape}")
    print(f"New rows shape: {secondary_data.shape}")

    highest_icsad_id = primary_data['icsad_ID'].max()
    print(f"Highest icsad_ID in {file_path}: {highest_icsad_id}")

    # Merge new rows based on icsad_ID
    combined_data = primary_data.merge(secondary_data, on='icsad_ID', how='outer', suffixes=('', '_new'))

    # Ensure all required columns are present in the combined data
    for column in required_columns:
        if column not in combined_data.columns:
            combined_data[column] = pd.NA

    # Drop duplicate columns if suffix is applied
    for column in combined_data.columns:
        if column.endswith('_new'):
            original_column = column[:-4]
            combined_data[original_column] = combined_data[original_column].combine_first(combined_data[column])
            combined_data.drop(columns=[column], inplace=True)

    # Reorder columns to match the primary data
    combined_data = combined_data[primary_data.columns]

    print(f"Combined data shape: {combined_data.shape}")

    save_data(combined_data, file_path)
    print(f"New rows merged side-by-side in {file_path}.")

try:
    specific_file_path = './official_part_1.csv'
    secondary_csv_path = 'CISA2.csv'
    drop_column = 'License'
    required_columns = ['icsad_ID', 'Original_Release_Date', 'Last_Updated', 'Year', 'ICS-CERT_Number', 'ICS-CERT_Advisory_Title', 'Vendor', 'Product', 'Products_Affected', 'CVE_Number', 'Cumulative_CVSS', 'CVSS_Severity', 'CWE_Number', 'Critical_Infrastructure_Sector', 'Product_Distribution', 'Company_Headquarters']
    append_data(specific_file_path, secondary_csv_path, required_columns, drop_column)
except FileNotFoundError as e:
    print(str(e))
except Exception as e:
    print("An unexpected error occurred:", str(e))
