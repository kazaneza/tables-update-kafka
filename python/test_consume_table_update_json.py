from kafka import KafkaConsumer
import json
import base64

# Kafka bootstrap server address and topic name
bootstrap_servers = '10.24.36.25:35002'
topic_name = 'table-update'

# Create a kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='table-update'
)

# File to store JSON data
output_file = 'kafka_messages.json'

try:
    message_count = 0
    messages = []
    for message in consumer:
        #convert message to base64 string
        base64_message = base64.b64encode(message.value).decode('utf-8')
        print(f"Received message: {base64_message}")
        messages.append(base64_message)
        message_count += 1

        if message_count >= 3:
            break
finally:
    consumer.close()

# Write messages to a JSON file
with open(output_file, 'w') as file:
    json.dump(messages, file, indent=4)

print(f"Consumed {message_count} messages and written to {output_file}")