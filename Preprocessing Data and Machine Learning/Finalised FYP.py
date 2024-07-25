#!/usr/bin/env python
# coding: utf-8

# # Include dependencies in PATH

# In[1]:


# Include dependencies in PATH
import sys
sys.path.append('/home/stud22015337/Desktop/Ivan/lib/python3.8/site-packages')
import pandas as pd
import numpy as np
import datetime as dt
import os
import csv
from dateutil.relativedelta import relativedelta
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt


# # Functions

# In[2]:


def readfile(file):
    row_numbers = []
    split = []

    for idx, value in enumerate(file.iloc[:,0]):
        if pd.notnull(value):
            row_numbers.append(idx)

    row_numbers.append("END")

    for index, item in enumerate(row_numbers):
        if row_numbers[index+1] == "END":
            no_of_rows = 0
            slice_df = file.iloc[row_numbers[index]:row_numbers[index]+1,:]

            # Check if the slice is empty
            if not slice_df.empty:
                while len(slice_df) > 0:
                    slice_df = file.iloc[row_numbers[index]+no_of_rows:row_numbers[index]+1+no_of_rows,:]
                    no_of_rows += 1
                no_of_rows -= 1
                split.append(file.iloc[row_numbers[index]:row_numbers[index]+no_of_rows,:])
            break
        else:
            new_x = row_numbers[index+1]
            slice_df = file.iloc[item:new_x,:]

            # Check if the slice is empty
            if not slice_df.empty:
                split.append(slice_df)

    return split

def findbydate(data, month):
    date_1 = dt.datetime.now()
    print("[+] Current Date: ", date_1)
    arr = []
    
    for index, i in enumerate(data):
        # Assuming `data` is a list of pandas DataFrames or similar structures
        event_date = i.iloc[0]['date']
#         print(event_date,index)
        if pd.isna(event_date):  # Check if the date is NaN
#             print(f"[+] Event date is non-existent in dataset, please check id", i.iloc[0,0])
            test=""
        else:
            event_date_str = str(event_date)
#             print(event_date_str, index)

            try:
                # Try parsing date in %d/%m/%Y format
                date_2 = dt.datetime.strptime(event_date_str, '%d/%m/%Y')
            except ValueError:
                try:
                    # If parsing fails, try parsing date in %Y-%m-%d format
                    date_2 = dt.datetime.strptime(event_date_str, '%Y-%m-%d')
                except ValueError as e:
                    print(f"[+] Error parsing date at index {index}: {e}")
                    continue
            difference = relativedelta(date_1, date_2)
            calculate_difference = difference.years * 12 + difference.months
            if calculate_difference < month:
                arr.append(i)
    
    print("[+] Successfully filtered by date")
    return arr

def ioccounter(dataset, column_name):

    total_counts = {}
    
    for data in dataset:
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Each element in dataset should be a DataFrame")
        # Check if 'Attribute_type' column exists in the DataFrame
        if 'attribute_type' not in data.columns:
            raise ValueError("DataFrame does not have 'Attribute_type' column")
        
        # Filter the DataFrame by column_name
        column_df = data[data['attribute_type'] == column_name]
        
        # Check if 'Attribute_value' column exists in the filtered DataFrame
        if 'attribute_value' not in column_df.columns:
            raise ValueError(f"DataFrame does not have 'Attribute_value' column after filtering by {column_name}")
        
        # Count occurrences of each value in 'Attribute_value'
        column_counts = column_df['attribute_value'].value_counts().to_dict()
        
        # Update total_counts with counts from current DataFrame
        for value, count in column_counts.items():
            if value in total_counts:
                total_counts[value] += count
            else:
                total_counts[value] = count
        sorted_total_counts = dict(sorted(total_counts.items(), key=lambda item: item[1], reverse=True))


    return sorted_total_counts

def combine_csv(directory,filename):
    arr = pd.DataFrame()
    files = os.listdir(directory)
    for i in range(1,len(files),1):
        filenamefinal = directory+"/"+filename+str(i)+".csv"
        df = pd.read_csv(filenamefinal,low_memory=False)
        arr = pd.concat([arr,df])
        print("[+] Successfully combined filename",filenamefinal)
    return arr

def baseline1(row):
    return {
        'attribute_id': row['attribute_id'], 
        'attribute_type': row['attribute_type'], 
        'attribute_category': row['attribute_category'], 
        'attribute_value': row['attribute_value'],
        'country_name': row['Country Name'],
        'latitude': row['Latitude'],
        'longitude': row['Longitude']  
    }
def cisareader(arr):
    for i in arr:
        test = i.iloc[:,27:42]
        print(type(test))
        break
    
        
def createcombinedcsv(arr,filename):
    # check for existing file with same name
    os.listdir()
    # if yes, delete file
    if os.path.isfile(filename):
    # If the file exists, delete it
        os.remove(filename)
        print(f"{filename} has been deleted.")
    else:
        print(f"{filename} does not exist.")
    
    
    # For charlene grafana
    for index,data in enumerate(arr):
        if index==0:
            data.to_csv(filename,index=False,mode='a')
        else:
            data.to_csv(filename,index=False,mode='a',header=False)
    print("[+] New dataset combined done")

# Used to extract solely the CISA table, currently not in use 
    
# def cisareader(arr):
#     df = pd.DataFrame(arr)
#     columns_to_extract = ['icsad_ID','icsad_ID',
#        'Original_Release_Date', 'Last_Updated', 'Year', 'ICS-CERT_Number',
#        'ICS-CERT_Advisory_Title', 'Vendor', 'Product', 'Products_Affected',
#        'CVE_Number', 'Cumulative_CVSS', 'CVSS_Severity', 'CWE_Number',
#        'Critical_Infrastructure_Sector', 'Product_Distribution',
#        'Company_Headquarters','EPSS Scores','Percentiles','Dates']
#     result = []
#     for index, row in df.iterrows():
#         row_data = [row[col] for col in columns_to_extract]
#         result.append(row_data)
#     return result

def custom_date_parser(date_string):
    # Use a custom date parser to handle specific formats
    return pd.to_datetime(date_string, format='%d/%m/%Y')


# # Main (MISP)

# In[3]:


arr = combine_csv("official","official_part_")
df = readfile(arr)
datefilter = findbydate(df,6) # filter data by months 
#make cleanser function here 

# count = ioccounter(testing,"ip-dst")
createcombinedcsv(df,"combined_final.csv") # this is for charlwene 


# In[4]:


print("Number of events: ",len(df))
print("Number of events filtered by date:",len(datefilter))


# # CISA + EPSS handling (Currently not in use)

# In[5]:


# cisa = cisareader(arr)

# filename = 'cisa.csv'

# with open(filename, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     # Write each list as a row
#     for row in cisa:
#         writer.writerow(row)

# print(f"Data written to {filename} successfully.")


# # Creating a baseline (IP-src addresses baseline)

# In[7]:


results = []

for i in df:
    eventid = i.iloc[0, 0]
    date = i.iloc[0, 1]
    orgid = i.iloc[0, 2]
    orgname = i.iloc[0, 3]
    orgcid = i.iloc[0, 4]
    orgcname = i.iloc[0, 5]
    eventname = i.iloc[0, 6]
    threatlevel = i.iloc[0, 7]
    time = i.iloc[0, 9]
    
    # Apply the baseline1 function
    test = i.apply(baseline1, axis=1)
   
    for index, row in test.items():
        result_row = {
            'eventid': eventid, 
            'date': date, 
            'orgid': orgid, 
            'orgname': orgname, 
            'orgcid': orgcid, 
            'orgcname': orgcname, 
            'eventname': eventname, 
            'threatlevel': threatlevel, 
            'time': time
        }
        
        # Update the result_row dictionary with values returned by baseline1
        result_row.update(row)
        if result_row['attribute_type'] == 'ip-src':
            results.append(result_row)

# Convert the results list to a DataFrame and save to CSV
final_df = pd.DataFrame(results)
if os.path.isfile("preprocess-ip-src.csv"):
# If the file exists, delete it
    os.remove("preprocess-ip-src.csv")
    print("preprocess-ip-src.csv has been deleted.")
else:
    print("preprocess-ip-src.csv does not exist.")
final_df.to_csv("preprocess-ip-src.csv")


# # Training and saving the model

# In[8]:


# Load your dataset
data = pd.read_csv("preprocess-ip-src.csv",low_memory=False)

# Assuming 'value' is the column with numeric data for anomaly detection
from sklearn.preprocessing import LabelEncoder

# Assuming attribute_value is a categorical column in your DataFrame
encoder = LabelEncoder()
data['attribute_value_encoded'] = encoder.fit_transform(data['attribute_value'])
data['eventid_encoded'] = encoder.fit_transform(data['eventid'])
data['date_encoded'] = encoder.fit_transform(data['date'])
data['orgid_encoded'] = encoder.fit_transform(data['orgid'])
data['orgname_encoded'] = encoder.fit_transform(data['orgname'])
data['orgcid_encoded'] = encoder.fit_transform(data['orgcid'])
data['orgcname_encoded'] = encoder.fit_transform(data['orgcname'])
data['eventname_encoded'] = encoder.fit_transform(data['eventname'])
data['threatlevel_encoded'] = encoder.fit_transform(data['threatlevel'])
data['time_encoded'] = encoder.fit_transform(data['time'])
data['attributeid_encoded'] = encoder.fit_transform(data['attribute_id'])
data['attributetype_encoded'] = encoder.fit_transform(data['attribute_type'])
data['attributecategory_encoded'] = encoder.fit_transform(data['attribute_category'])
data['countryname_encoded'] = encoder.fit_transform(data['country_name'])

# Now use 'attribute_value_encoded' for anomaly detection
X = data[['attribute_value_encoded']]


# Initialize Isolation Forest model
model = IsolationForest(contamination=0.1)  # Adjust contamination based on expected outlier percentage

# Fit model and predict anomalies
model.fit(X)
predictions = model.predict(X)

# Anomalies are where predictions == -1
anomalies = data[predictions == -1]

# Optionally, visualize or further analyze anomalies
if os.path.isfile("IP-src anomalies.csv"):
# If the file exists, delete it
    os.remove("IP-src anomalies.csv")
    print("IP-src anomalies.csv has been deleted.")
else:
    print("IP-src anomalies.csv does not exist.")
anomalies.to_csv("IP-src anomalies.csv",index=False)
print(anomalies)


# # Creating a baseline (IP-dst addresses baseline)

# In[9]:


results = []

for i in df:
    eventid = i.iloc[0, 0]
    date = i.iloc[0, 1]
    orgid = i.iloc[0, 2]
    orgname = i.iloc[0, 3]
    orgcid = i.iloc[0, 4]
    orgcname = i.iloc[0, 5]
    eventname = i.iloc[0, 6]
    threatlevel = i.iloc[0, 7]
    time = i.iloc[0, 9]
    
    # Apply the baseline1 function
    test = i.apply(baseline1, axis=1)
   
    for index, row in test.items():
        result_row = {
            'eventid': eventid, 
            'date': date, 
            'orgid': orgid, 
            'orgname': orgname, 
            'orgcid': orgcid, 
            'orgcname': orgcname, 
            'eventname': eventname, 
            'threatlevel': threatlevel, 
            'time': time
        }
        
        # Update the result_row dictionary with values returned by baseline1
        result_row.update(row)
        if result_row['attribute_type'] == 'ip-dst':
            results.append(result_row)

# Convert the results list to a DataFrame and save to CSV
final_df = pd.DataFrame(results)
if os.path.isfile("preprocess-ip-dst.csv"):
# If the file exists, delete it
    os.remove("preprocess-ip-dst.csv")
    print("preprocess-ip-dst.csv has been deleted.")
else:
    print("preprocess-ip-dst.csv does not exist.")
final_df.to_csv("preprocess-ip-dst.csv")


# # Training and saving the model

# In[17]:


# Load your dataset
data = pd.read_csv("preprocess-ip-dst.csv",low_memory=False)

# Assuming 'value' is the column with numeric data for anomaly detection
from sklearn.preprocessing import LabelEncoder

# Assuming attribute_value is a categorical column in your DataFrame
encoder = LabelEncoder()
data['attribute_value_encoded'] = encoder.fit_transform(data['attribute_value'])
data['eventid_encoded'] = encoder.fit_transform(data['eventid'])
data['date_encoded'] = encoder.fit_transform(data['date'])
data['orgid_encoded'] = encoder.fit_transform(data['orgid'])
data['orgname_encoded'] = encoder.fit_transform(data['orgname'])
data['orgcid_encoded'] = encoder.fit_transform(data['orgcid'])
data['orgcname_encoded'] = encoder.fit_transform(data['orgcname'])
data['eventname_encoded'] = encoder.fit_transform(data['eventname'])
data['threatlevel_encoded'] = encoder.fit_transform(data['threatlevel'])
data['time_encoded'] = encoder.fit_transform(data['time'])
data['attributeid_encoded'] = encoder.fit_transform(data['attribute_id'])
data['attributetype_encoded'] = encoder.fit_transform(data['attribute_type'])
data['attributecategory_encoded'] = encoder.fit_transform(data['attribute_category'])
data['countryname_encoded'] = encoder.fit_transform(data['country_name'])

# Now use 'attribute_value_encoded' for anomaly detection
X = data[['attribute_value_encoded']]


# Initialize Isolation Forest model
model = IsolationForest(contamination=0.5)  # Adjust contamination based on expected outlier percentage

# Fit model and predict anomalies
model.fit(X)
predictions = model.predict(X)

# Anomalies are where predictions == 1, to find ip addresses that keep getting attacked
anomalies = data[predictions == 1]

# Optionally, visualize or further analyze anomalies
if os.path.isfile("IP-dst anomalies.csv"):
# If the file exists, delete it
    os.remove("IP-dst anomalies.csv")
    print("IP-dst anomalies.csv has been deleted.")
else:
    print("IP-dst anomalies.csv does not exist.")
anomalies.to_csv("IP-dst anomalies.csv",index=False)
print(anomalies)


# In[ ]:




