

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