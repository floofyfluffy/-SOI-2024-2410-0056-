import requests
import pandas as pd
import re
import os
from datetime import datetime
import glob

def find_and_sort_csv_files(directory='.', pattern='official_part_*.csv'):
    """ Find and sort CSV files by the numeric part of their filename. """
    files = glob.glob(os.path.join(directory, pattern))
    files.sort(key=lambda x: int(re.search(r'(\d+)', x.split('_')[-1]).group()))
    return files

def extract_cves_from_csv(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        for column in ['EPSS Scores', 'Percentiles', 'Dates']:
            if column not in df.columns:
                df[column] = pd.NA
    except Exception as e:
        print(f"Failed to read {file_path} due to: {str(e)}")
        return None
    return df

def extract_and_filter_cves(df):
    cve_pattern = r'CVE-\d{4}-\d{4,7}'
    cve_list = set()
    for index, row in df.iterrows():
        found_cves = re.findall(cve_pattern, row.to_string())
        if found_cves:
            existing_scores = str(row['EPSS Scores'])
            for cve in found_cves:
                if cve not in existing_scores:
                    cve_list.add(cve)
    return list(cve_list)

def query_epss_api(cve_list, batch_size=50):
    if not cve_list:
        print("No CVEs to query. Skipping API calls.")
        return {}, {'total': 0, 'successful': 0, 'failed': 0}
    epss_scores = {}
    stats = {'total': 0, 'successful': 0, 'failed': 0}
    url = "https://api.first.org/data/v1/epss"
    for i in range(0, len(cve_list), batch_size):
        batch = cve_list[i:i+batch_size]
        response = requests.get(url, params={'cve': ','.join(batch)})
        stats['total'] += len(batch)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data:
                for item in data['data']:
                    epss_scores[item['cve']] = {
                        'epss': item.get('epss', ''),
                        'percentile': item.get('percentile', ''),
                        'date': item.get('date', '')
                    }
                    stats['successful'] += 1
        else:
            stats['failed'] += len(batch)
    return epss_scores, stats

def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%d/%m/%Y')
    except ValueError:
        return date_str

def append_scores_to_csv(df, epss_scores, file_path):
    for index, row in df.iterrows():
        found_cves = re.findall(r'CVE-\d{4}-\d{4,7}', row.to_string())
        for cve in found_cves:
            data = epss_scores.get(cve, {'epss': '', 'percentile': '', 'date': ''})
            if pd.isna(df.at[index, 'EPSS Scores']):
                df.at[index, 'EPSS Scores'] = f"{cve}:{data['epss']}"
            else:
                df.at[index, 'EPSS Scores'] += f", {cve}:{data['epss']}"
            if pd.isna(df.at[index, 'Percentiles']):
                df.at[index, 'Percentiles'] = f"{cve}:{data['percentile']}"
            else:
                df.at[index, 'Percentiles'] += f", {cve}:{data['percentile']}"
            if pd.isna(df.at[index, 'Dates']):
                df.at[index, 'Dates'] = f"{cve}:{format_date(data['date'])}"
            else:
                df.at[index, 'Dates'] += f", {cve}:{format_date(data['date'])}"
    df.to_csv(file_path, index=False)
    print("CSV file updated.")

def print_summary(stats, file_path):
    print(f"Processing file: {file_path}")
    print("Summary of EPSS Queries:")
    print(f"Total CVEs queried: {stats['total']}")
    print(f"Successfully queried: {stats['successful']}")
    print(f"Failed to query: {stats['failed']}")

# Main processing flow
file_paths = find_and_sort_csv_files()  # Adjust the directory if needed
for file_path in file_paths:
    original_df = extract_cves_from_csv(file_path)
    if original_df is not None:
        cve_ids = extract_and_filter_cves(original_df)
        if cve_ids:
            epss_results, statistics = query_epss_api(cve_ids)
            append_scores_to_csv(original_df, epss_results, file_path)
            print_summary(statistics, file_path)
        else:
            print("No new CVEs to query. All necessary data is already in the CSV.")
