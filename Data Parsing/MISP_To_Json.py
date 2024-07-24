import json
import pandas as pd
from datetime import datetime, date
from pymisp import PyMISP

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()  # Convert datetime/date to ISO8601 string
    raise TypeError("Type not serializable")


# MISP connection details
misp_url = 'https://misp.otisac.org/'
misp_key = 'qCrtLX4ubnNAnpvFbgHwDf5KhsnDvfIS5HVRvSaM'  # Replace with your actual MISP API key
misp_verifycert = True

# Create a PyMISP object
misp = PyMISP(misp_url, misp_key, misp_verifycert)

# Number of events to fetch per run
num_events_to_fetch = 1000

# Path to the JSON file
file_path = 'official.json'

# Check if JSON file exists and load its contents
try:
    with open(file_path, 'r') as file:
        existing_events = json.load(file)
    existing_event_ids = {int(event['Event']['id']) for event in existing_events}
    latest_event_id = max(existing_event_ids)
    print(f"Latest event ID found: {latest_event_id}")
except FileNotFoundError:
    print("JSON file not found. Creating a new one.")
    existing_events = []
    existing_event_ids = set()
    latest_event_id = 0  # Start from 0 if the file doesn't exist
except ValueError:
    print("JSON file is empty or corrupted. Creating a new one.")
    existing_events = []
    existing_event_ids = set()
    latest_event_id = 0  # Start from 0 if the file is corrupted

# List to hold new event data
new_events = []

# Loop to fetch the next set of events
for event_id in range(latest_event_id + 1, latest_event_id + 1 + num_events_to_fetch):
    try:
        # Fetch the specific event
        response = misp.get_event(event_id, pythonify=True)

        # Basic event data
        event_data = {
            'Event': {
                'id': response.id,
                'date': response.date,
                'Org.id': response.org.id if response.org else None,
                'Org.name': response.org.name if response.org else None,
                'Orgc.id': response.orgc.id if response.orgc else None,
                'Orgc.name': response.orgc.name if response.orgc else None,
                'info': response.info,
                'threat_level_id': response.threat_level_id,
                "publish_timestamp": response.publish_timestamp,
                "timestamp": response.timestamp,
                'Attributes': [],
                'RelatedEvents': [],
                'Tags': []
            }
        }

        # Attributes data
        for attribute in response.attributes:
            event_data['Event']['Attributes'].append({
                'attribute_id': attribute.id,
                'attribute_type': attribute.type,
                'attribute_category': attribute.category,
                'attribute_timestamp': attribute.timestamp,
                'attribute_value': attribute.value
            })

        # Related events data
        if hasattr(response, 'RelatedEvent'):
            for related_event in response.RelatedEvent:
                event_data['Event']['RelatedEvents'].append({
                    'related_event_id': related_event['Event']['id'],
                })

        # Tags data
        for tag in response.tags:
            event_data['Event']['Tags'].append({
                'tag_id': tag.id,
                'tag_name': tag.name,
                'tag_is_galaxy': tag.is_galaxy,
            })

             # Galaxy data including clusters
        if hasattr(response, 'Galaxy'):
            for galaxy in response.Galaxy:
                event_data['Event'].setdefault('Galaxies', []).append({
                    'galaxy_id': galaxy.id,
                    'galaxy_name': galaxy.name,
                    'galaxy_type': galaxy.type,
                    'galaxy_description': galaxy.description,
                    'Clusters': []
                })

                # Check and append GalaxyCluster data
                if hasattr(galaxy, 'GalaxyCluster'):
                    for cluster in galaxy.GalaxyCluster:
                        event_data['Event']['Galaxies'][-1]['Clusters'].append({
                            'cluster_id': cluster.id,
                            'cluster_type': cluster.type,
                            'cluster_value': cluster.value,
                            'cluster_description': cluster.description,
                            'cluster_galaxy_id': cluster.galaxy_id,
                        })



        new_events.append(event_data)  # Append the event data to the list of new events
        print(f"Fetched event ID: {event_id}")

    except Exception as e:
        print(f"Failed to fetch event ID: {event_id}, Error: {e}")

# Merge new events with existing events
all_events = existing_events + new_events

# Convert all events to JSON and save/print
json_output = json.dumps(all_events, indent=4, default=json_serial)
#print(json_output)

# Write to a JSON file
with open(file_path, 'w') as file:
    file.write(json_output)
