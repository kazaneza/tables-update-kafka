import json
from nested import get_value_from_nested_json

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