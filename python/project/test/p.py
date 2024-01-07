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



























def get_value_from_nested_json(data, key):
    keys = key.split('.')
    for k in keys:
        if isinstance(data, list):  # Handle list of dictionaries
            new_data = []
            for sub_data in data:
                if isinstance(sub_data, dict) and k in sub_data:
                    new_data.append(sub_data[k])
                elif isinstance(sub_data, dict):
                    for sub_key, sub_value in sub_data.items():
                        if isinstance(sub_value, dict) and k in sub_value:
                            new_data.append(sub_value[k])
            data = new_data
        elif isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None

    # Flatten the list if it contains sublists
    if isinstance(data, list) and all(isinstance(elem, list) for elem in data):
        data = [item for sublist in data for item in sublist]

    return data


























def get_value_from_nested_json(data, key):
    keys = key.split('.')
    for k in keys:
        if isinstance(data, list):  # Handle list of dictionaries
            new_data = []
            for sub_data in data:
                if isinstance(sub_data, dict) and k in sub_data:
                    new_data.append(sub_data[k])
                elif isinstance(sub_data, dict):
                    for sub_key, sub_value in sub_data.items():
                        if isinstance(sub_value, dict) and k in sub_value:
                            new_data.append(sub_value[k])
            data = new_data
        elif isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None

    # Flatten the list if it contains sublists
    if isinstance(data, list) and all(isinstance(elem, list) for elem in data):
        data = [item for sublist in data for item in sublist]

    return data











# Assuming the function is imported or defined in your script

# Sample message structure
sample_message = {
    "payload": {
        "ARRAY_LegalId": [
            {
                "RECORD_LegalId": {
                    "LegalId": "1197370090468084",
                    "LegalDocName": "NATIONAL.ID",
                    "LegalHolderName": null,
                    "LegalIssAuth": "NIDA",
                    "LegalIssDate": "20080101",
                    "LegalExpDate": null
                }
            }
        ],
        # ... other fields ...
    }
}

# Test extraction of LegalDocName
path_to_legal_doc_name = "payload.ARRAY_LegalId.RECORD_LegalId.LegalDocName"
extracted_legal_doc_names = get_value_from_nested_json(sample_message, path_to_legal_doc_name)
print("Extracted Legal Doc Names:", extracted_legal_doc_names)











