def process_message(msg, config):
    # ... [existing code]

    for field in selected_fields:
        if field == 'message_json':
            extracted_fields[field] = json.dumps(message_fields)
        else:
            extracted_value = get_value_from_nested_json(message_fields, field)
            if extracted_value is not None:
                extracted_fields[field] = extracted_value

    return extracted_fields['message_json'], extracted_fields
