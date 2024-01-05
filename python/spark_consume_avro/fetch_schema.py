import requests
import json  # Import the json module

def list_schema_registry_subjects(schema_registry_url):
    response = requests.get(f"{schema_registry_url}/subjects")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list subjects: {response.text}")

def fetch_schema_versions_for_subject(schema_registry_url, subject):
    response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get schema versions for subject {subject}: {response.text}")

def fetch_schema_for_subject_version(schema_registry_url, subject, version):
    response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/{version}")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get schema for subject {subject} version {version}: {response.text}")

schema_registry_url = "http://10.24.36.25:35003"
subjects = list_schema_registry_subjects(schema_registry_url)

all_schemas = {}

for subject in subjects:
    versions = fetch_schema_versions_for_subject(schema_registry_url, subject)
    all_schemas[subject] = {}
    for version in versions:
        schema = fetch_schema_for_subject_version(schema_registry_url, subject, version)
        all_schemas[subject][version] = schema

# Save the schema information to a file
with open('schema_registry_data.json', 'w') as file:
    json.dump(all_schemas, file, indent=4)

print("All schema information has been saved to 'schema_registry_data.json'")
