import pandas as pd
import glob
import os

def fill_down_column(dataframe, column_name, initial_value=None):
    """Fill down values in a specified column until a new value appears."""
    last_value = initial_value
    for index, value in dataframe[column_name].items():
        if pd.isna(value):
            dataframe.at[index, column_name] = last_value
        else:
            last_value = value
    return dataframe

def load_existing_dataframes(file_pattern='official_part_*.csv'):
    """Load existing DataFrames from CSV files matching the pattern."""
    csv_files = sorted(
        glob.glob(file_pattern),
        key=lambda x: int(os.path.splitext(x.split('_')[-1])[0]) if x.split('_')[-1].split('.')[0].isdigit() else -1
    )
    csv_files = [f for f in csv_files if f.split('_')[-1].split('.')[0].isdigit()]
    dataframes = [pd.read_csv(f, low_memory=False) for f in csv_files]
    return dataframes, csv_files

# Load existing dataframes
existing_dfs, existing_csv_files = load_existing_dataframes()

# Initialize the last attribute_timestamp value
last_timestamp_value = None

# Process each dataframe to update 'attribute_timestamp'
for i, df in enumerate(existing_dfs):
    if 'attribute_timestamp' in df.columns:
        # If this is not the first file and the first data row is empty, fill it with the last timestamp value
        if last_timestamp_value is not None and pd.isna(df['attribute_timestamp'].iloc[0]):
            df['attribute_timestamp'].iloc[0] = last_timestamp_value
        
        # Fill down the column values
        df = fill_down_column(df, 'attribute_timestamp', initial_value=last_timestamp_value)
        
        # Update the last timestamp value
        last_timestamp_value = df['attribute_timestamp'].iloc[-1]

        # Save the modified dataframe back to the CSV file
        df.to_csv(existing_csv_files[i], index=False)
        print(f"Updated 'attribute_timestamp' in {existing_csv_files[i]}")

print("All CSV files have been updated.")
