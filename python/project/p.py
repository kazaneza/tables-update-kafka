def get_value_from_nested_json(data, key):
    keys = key.split('.')
    for k in keys[:-1]:  # Process all but the last key
        if isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None

    last_key = keys[-1]  # Corrected this line
    if isinstance(data, list):
        # Join all values from the array into a single string
        return ', '.join([item.get(last_key, '') for item in data if isinstance(item, dict)])
    elif isinstance(data, dict) and last_key in data:
        return data[last_key]  # Handling for non-list data
    else:
        return None
