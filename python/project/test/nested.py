def get_value_from_nested_json(data, key):
    keys = key.split('.')
    for k in keys:
        if isinstance(data, list):  # Handle list of dictionaries
            new_data = []
            for sub_data in data:
                if isinstance(sub_data, dict) and k in sub_data:
                    new_data.append(sub_data[k])
                elif isinstance(sub_data, dict):
                    # Handling nested array within a dictionary
                    nested_data = sub_data.get(k)
                    if isinstance(nested_data, list):
                        for item in nested_data:
                            if isinstance(item, dict):
                                # If k is a key in this dictionary, extract its value
                                if k in item:
                                    new_data.append(item[k])
                                # Otherwise, iterate through its values which may be lists or dictionaries
                                else:
                                    for nested_key, nested_value in item.items():
                                        if isinstance(nested_value, list):
                                            for nested_item in nested_value:
                                                if isinstance(nested_item, dict) and k in nested_item:
                                                    new_data.append(nested_item[k])
                    elif isinstance(nested_data, dict):
                        new_data.extend(nested_data.values())
            data = new_data
        elif isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None

    # Flatten the list if it contains sublists
    if isinstance(data, list) and any(isinstance(elem, list) for elem in data):
        data = [item for sublist in data for item in sublist if isinstance(sublist, list)]

    return data
