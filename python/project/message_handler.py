import json

def get_value_from_nested_json(data, key):
    
    # Recursively fetch value from nested JSON.

    keys = key.split('.')
    for k in keys:
        if isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None
    return data

def process_message(msg, config):

    # Process the message and extract necessary fields based on the config.

    message_fields = msg.value() # Extract all fields from the message
    entity_name = message_fields.get('entityName')

    if entity_name not in config:
        return None, None # no error handle yet
    
    selected_fields = config[entity_name]["columns"]
    extracted_fields = {}

    for field in selected_fields:
        if field == 'message_json':
            extracted_fields[field] = json.dumps(message_fields)
        else:
            extracted_value = get_value_from_nested_json(message_fields, field)
            if extracted_value is not None:
                extracted_fields[field] = extracted_value

    return extracted_fields['message_json'], extracted_fields

# Load config
with open('config.json', 'r') as file:
    config = json.load(file)           