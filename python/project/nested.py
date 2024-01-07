def get_value_from_nested_json(data, path):
    def inner_extract(current_data, remaining_path):
        if not remaining_path:
            return current_data
        
        current_key = remaining_path[0]
        if isinstance(current_data, list):
            for item in current_data:
                if isinstance(item, dict) and current_key in item:
                    result = inner_extract(item[current_key], remaining_path[1:])
                    if result is not None:
                        return result if isinstance(result, list) else[result]
                elif isinstance(item, dict):
                    for sub_item in item.values():
                        result = inner_extract(sub_item, remaining_path)
                        if result is not None:
                            return result if isinstance(result, list) else [result]
                        
            return None
        if isinstance(current_data, dict) and current_key in current_data:
            return inner_extract(current_data[current_key], remaining_path[1:])
        else:
            return None
        
    final_result = inner_extract(data, path.split('.'))
    #Return the first element if the result is a list with only one item
    if isinstance(final_result, list) and len(final_result) == 1:
        return final_result[0]
    return final_result