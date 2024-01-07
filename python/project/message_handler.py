import json

def get_value_from_nested_json(data, key):

    # Recursively fetch value from nested JSON
    keys = key.split('.')
    for i, k in enumerate(keys):
        if isinstance(data, dict) and k in data:
            data = data[k]
        elif isinstance(data, list) and i < len(keys) - 1:
            # Extract a list of values for the next key in all array elements
            next_key = keys[i + 1]
            return [element[next_key] for element in data if next_key in element]
        else:
            return None
    return data


def process_message(msg, config):

    # Process the message and extract fields based on config
    message_fields = msg.value() # Extract all fields from the message
    entity_name = message_fields.get('entityName')

    if entity_name not in config:
        return None, None # No error handling yet
    
    selected_fields = config[entity_name]["columns"]
    extracted_fields = {}

    for field in selected_fields:
        if field == 'message_json':
            extracted_fields[field] = json.dumps(message_fields)
        else:
            extracted_value = get_value_from_nested_json(message_fields, field)
            if extracted_value is not None:
                if isinstance(extracted_value, list):
                    # Convert list to JSON string for database insertaion
                    extracted_fields[field] = json.dumps(extracted_value)
                else:
                    extracted_fields[field] = extracted_value
    
    return extracted_fields.get('message_json', None), extracted_fields
    

# Load config
with open('config.json', 'r') as file:
    config = json.load(file)