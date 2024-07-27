import json
import datetime
from pymisp import PyMISP

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime.date):
        return obj.isoformat()  # Convert datetime/date to ISO8601 string
    raise TypeError("Type not serializable")

# MISP connection details
misp_url = 'https://misp.otisac.org/'
misp_key = ''  # Replace with your actual MISP API key
misp_verifycert = True

# Create a PyMISP object
misp = PyMISP(misp_url, misp_key, misp_verifycert)

# Number of event IDs to attempt to fetch per run
num_event_ids_to_attempt = 1000
# Counter for consecutive missing events
missing_event_count = 0
# Maximum allowed consecutive missing events
max_missing_events = 10

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

# Loop to fetch the next set of event IDs
for event_id in range(latest_event_id + 1, latest_event_id + 1 + num_event_ids_to_attempt):
    try:
        # Fetch the specific event
        response = misp.get_event(event_id, pythonify=True)
        
        # Define event_data here to build a dictionary from the response
        event_data = {
            'Event': {
                'id': response.id,
                'date': response.date.isoformat(),
                'Org': {
                    'id': response.org.id if response.org else None,
                    'name': response.org.name if response.org else None,
                },
                'info': response.info,
                'threat_level_id': response.threat_level_id,
                'publish_timestamp': response.publish_timestamp,
                'timestamp': response.timestamp,
                'Attributes': [],
                'Tags': [],
                'Galaxies': []
            }
        }

        # Append attributes if available
        if hasattr(response, 'attributes'):
            for attribute in response.attributes:
                event_data['Event']['Attributes'].append({
                    'id': attribute.id,
                    'type': attribute.type,
                    'category': attribute.category,
                    'value': attribute.value
                })

        # Append tags if available
        if hasattr(response, 'tags'):
            for tag in response.tags:
                event_data['Event']['Tags'].append({
                    'id': tag.id,
                    'name': tag.name,
                })

        # Append galaxies if available
        if hasattr(response, 'galaxies'):
            for galaxy in response.galaxies:
                galaxy_data = {
                    'id': galaxy.id,
                    'name': galaxy.name,
                    'type': galaxy.type,
                    'description': galaxy.description,
                    'Clusters': []
                }
                if hasattr(galaxy, 'GalaxyCluster'):
                    for cluster in galaxy.GalaxyCluster:
                        galaxy_data['Clusters'].append({
                            'id': cluster.id,
                            'type': cluster.type,
                            'value': cluster.value,
                            'description': cluster.description
                        })
                event_data['Event']['Galaxies'].append(galaxy_data)

        new_events.append(event_data)  # Append the event data to the list of new events
        print(f"Fetched event ID: {event_id}")
        # Reset missing event count after successful fetch
        missing_event_count = 0

    except AttributeError as e:
        # Increment the missing event count
        missing_event_count += 1
        print(f"Failed to fetch event ID: {event_id}, Error: {e}")
        # Break the loop if the count of consecutive missing events exceeds the threshold
        if missing_event_count >= max_missing_events:
            print("Consecutive missing event limit reached; stopping fetch.")
            break
    except Exception as e:
        print(f"Unexpected error for event ID: {event_id}, Error: {e}")
        break

# Merge new events with existing events
all_events = existing_events + new_events

# Convert all events to JSON and save/print
json_output = json.dumps(all_events, indent=4, default=json_serial)
#print(json_output)

# Write to a JSON file
with open(file_path, 'w') as file:
    file.write(json_output)
