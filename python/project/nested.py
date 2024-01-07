
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