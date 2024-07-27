import pandas as pd
import os
import re

# Directory where your files are located (adjust as necessary)
directory = '.'

# Compile a regex pattern to match files
pattern = re.compile(r'official_part_\d+\.csv')

# List all files in the directory
files = os.listdir(directory)

for filename in files:
    if pattern.match(filename):
        input_file = os.path.join(directory, filename)
        output_file = os.path.join(directory, filename)

        # Load the CSV with the correct encoding 'utf-8'
        df = pd.read_csv(input_file, encoding='utf-8')

        # Optionally, check the first few rows to make sure it reads correctly
        print(f'Preview of {input_file}:')
        print(df.head())

        # Save the CSV with the same encoding 'utf-8-sig'
        df.to_csv(output_file, encoding='utf-8-sig', index=False)
        print(f'Saved re-encoded file to {output_file}')
