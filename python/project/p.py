from nested import get_value_from_nested_json

# Sample message
message = {
    "eventId": "7ec7fd93-9473-4458-a0fa-abbf9529138f",
    # ... other fields ...
    "payload": {
        "recId": "1266740",
        "CustomerCode": None,
        "Mnemonic": "BK1266740",
        "ARRAY_ShortName": [
            {"ShortName": "NTAWIYOBERA ESTHER"}
        ],
        # ... other arrays ...
    }
}

# Path to extract ShortName
path_to_shortname = "payload.ARRAY_ShortName.ShortName"

# Extract ShortName
extracted_short_names = get_value_from_nested_json(message, path_to_shortname)
print("Extracted Short Names:", extracted_short_names)




















def get_value_from_nested_json(data, key):
    keys = key.split('.')
    for k in keys:
        if isinstance(data, list):  # Handle list of dictionaries
            data = [sub_data.get(k) for sub_data in data if k in sub_data]
        elif isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None

    # Flatten the list if it contains sublists
    if isinstance(data, list) and all(isinstance(elem, list) for elem in data):
        data = [item for sublist in data for item in sublist]

    return data
