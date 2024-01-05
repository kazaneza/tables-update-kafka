import json

def process_message(msg):
   
    message_fields = msg.value()    # Extract all fields from the message
    message_json = json.dumps(message_fields, default=str) # Process fields as needed

    return message_json, message_fields