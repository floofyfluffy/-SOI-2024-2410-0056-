import pandas as pd
import json
import math
import os
import glob

def clean_text(text):
    """Remove problematic characters and sanitize text for Excel."""
    if isinstance(text, str):
        return ''.join(c for c in text if c.isprintable()).replace('\r', '').replace('\n', ' ').replace('\t', ' ')
    return text

def load_existing_dataframes(file_pattern='./official/official_part_*.csv'):
    """Load existing DataFrames from CSV files matching the pattern."""
    csv_files = sorted(
        glob.glob(file_pattern),
        key=lambda x: int(os.path.splitext(x.split('_')[-1])[0]) if x.split('_')[-1].split('.')[0].isdigit() else -1
    )
    csv_files = [f for f in csv_files if f.split('_')[-1].split('.')[0].isdigit()]
    dataframes = [pd.read_csv(f, low_memory=False) for f in csv_files]
    return dataframes, csv_files

def get_latest_event_id_from_latest_file(csv_files):
    """Get the largest event ID from the latest existing file."""
    if not csv_files:
        return -1, ""
    
    latest_csv_file = csv_files[-1]
    df = pd.read_csv(latest_csv_file, low_memory=False)
    latest_event_id = -1
    if 'event_id' in df.columns:
        latest_event_id = pd.to_numeric(df['event_id'], errors='coerce').max()
    return latest_event_id, latest_csv_file

def append_data_to_existing_files(chunks, csv_files, num_rows_per_file):
    """Append data to existing CSV files."""
    file_index = len(csv_files)
    if csv_files:
        last_csv_file = csv_files[-1]
        last_df = pd.read_csv(last_csv_file, low_memory=False)
        initial_rows = len(last_df)
        remaining_space = num_rows_per_file - initial_rows
        
        if remaining_space > 0 and chunks:
            chunk = chunks.pop(0)
            rows_to_append = min(remaining_space, len(chunk))
            remaining_chunk = chunk.iloc[rows_to_append:]
            last_df = pd.concat([last_df, chunk.iloc[:rows_to_append]], ignore_index=True)
            last_df.to_csv(last_csv_file, index=False)
            print(f"Appended {rows_to_append} rows to {last_csv_file}")
            print(f"Initial rows in {last_csv_file}: {initial_rows}, Rows after appending: {len(last_df)}")
            if not remaining_chunk.empty:
                chunks.insert(0, remaining_chunk)
                print(f"Exceeding {num_rows_per_file} rows in {last_csv_file}, creating new file for remaining data.")
        else:
            print(f"No space left in {last_csv_file}. Creating new file for remaining data.")
                
    for chunk in chunks:
        file_index += 1
        csv_file_path = f'./official/official_part_{file_index}.csv'
        chunk.to_csv(csv_file_path, index=False)
        print(f"Data saved to {csv_file_path}")

def expand_nested_fields(df_event, event_data, key):
    """Expand nested fields like 'Attributes' into separate rows."""
    expanded_rows = []
    if key in event_data:
        for item in event_data[key]:
            df_temp = pd.json_normalize(item)
            df_temp = df_temp.applymap(clean_text)
            df_temp['event_id'] = df_event['event_id'].iloc[0]  # Associate with the main event ID
            expanded_rows.append(df_temp)
    if expanded_rows:
        return pd.concat(expanded_rows, ignore_index=True)
    return pd.DataFrame()

# Load JSON data
with open('official.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Load existing dataframes if any
existing_dfs, existing_csv_files = load_existing_dataframes()
latest_event_id, latest_event_file = get_latest_event_id_from_latest_file(existing_csv_files)

# If existing data is present, filter out older events
if latest_event_id != -1:
    data = [event for event in data if int(event['Event']['id']) > latest_event_id]
    print(f"Latest event_id found: {latest_event_id} in file: {latest_event_file}")
    print(f"Processing new events after filtering: {len(data)}")

all_events_df = []  # Store each event's DataFrame
for event in data:
    event_data = event['Event']
    
    # Normalize the main event data and rename the 'id' column to 'event_id'
    df_event = pd.json_normalize(event_data)
    df_event = df_event.applymap(clean_text)
    if 'id' in df_event.columns:
        df_event.rename(columns={'id': 'event_id'}, inplace=True)
    
    # Drop unnecessary columns from the main event data
    unnecessary_columns = ['Attributes', 'RelatedEvents', 'Tags', 'Galaxies', 'Clusters']
    df_event.drop(columns=[col for col in unnecessary_columns if col in df_event.columns], inplace=True)
    
    # Expand nested fields and concatenate them vertically
    df_combined = df_event
    for key in ['Attributes', 'RelatedEvents', 'Tags', 'Galaxies', 'Clusters']:
        df_expanded = expand_nested_fields(df_event, event_data, key)
        if not df_expanded.empty:
            df_combined = pd.concat([df_combined, df_expanded], ignore_index=True)
    
    # Append event DataFrame to the list
    all_events_df.append(df_combined)

# Concatenate all event DataFrames into a single DataFrame and reset index
if all_events_df:
    final_df = pd.concat(all_events_df, ignore_index=True)

    # Debug: Check if 'event_id' column exists
    if 'event_id' not in final_df.columns:
        print(final_df.columns)
        raise KeyError("'event_id' column is missing in the final DataFrame")

    # Replace subsequent duplicated values with NaN except for 'attribute_type' and 'attribute_category'
    columns_to_process = [col for col in final_df.columns if col not in ['attribute_type', 'attribute_category']]
    final_df[columns_to_process] = final_df[columns_to_process].mask(final_df[columns_to_process].eq(final_df[columns_to_process].shift()))

    # Define the maximum number of rows per file
    average_row_limit = 500000

    if existing_dfs:
        num_rows_per_file = sum(len(df) for df in existing_dfs) // len(existing_dfs)
    else:
        num_rows_per_file = average_row_limit

    # Split DataFrame into equally sized chunks
    chunks = [final_df.iloc[i:i + num_rows_per_file] for i in range(0, len(final_df), num_rows_per_file)]

    # Append or save each chunk to a CSV file
    append_data_to_existing_files(chunks, existing_csv_files, num_rows_per_file)
else:
    print("No new events to process.")
