def get_value_from_nested_json(data, path):
    def inner_extract(current_data, remaining_path):
        if not remaining_path:
            yield current_data
            return

        current_key = remaining_path[0]
        if isinstance(current_data, list):
            for item in current_data:
                if isinstance(item, dict) and current_key in item:
                    yield from inner_extract(item[current_key], remaining_path[1:])
                elif isinstance(item, dict):
                    for sub_item in item.values():
                        yield from inner_extract(sub_item, remaining_path)
        elif isinstance(current_data, dict) and current_key in current_data:
            yield from inner_extract(current_data[current_key], remaining_path[1:])

    final_result = list(inner_extract(data, path.split('.')))
    if len(final_result) == 1:
        return final_result[0]
    return final_result
