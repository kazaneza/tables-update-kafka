from kafka import KafkaConsumer

# Kafka bootstrap server address
bootstrap_servers = '10.24.36.25:35002'

# Kafka topic you want to consume
topic_name = 'table-update'

# Create a kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='table-update'
)

# Consume messages
try:
    message_count = 0
    for message in consumer:
        print(f"Received message: {message.value}")
        message_count +=1

        #Break after receiving 10 messages
        if message_count >= 3:
            break
finally:
    consumer.close()

print("Consumed 10 messages, exiting....")