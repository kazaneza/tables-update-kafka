import json

def process_message(msg):
    # Convert message to JSON
    message_json = {
        "key": msg.key(),
        "value": msg.value(),
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset()
    }

    # Extract additional fields
    processing_time = msg.value().get('processingTime', None)
    entity_name = msg.value().get('entityName', None)
    entity_id = msg.value().get('entityId', None)


    # Prepare Json data
    json_data = json.dumps(message_json, default=str)

    return json_data, processing_time, entity_name, entity_id