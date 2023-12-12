import logging
from confluent_kafka import Consumer, KafkaException
import sys
import json

# Python logging
logging.basicConfig(level=logging.DEBUG, filename='kafka_consumer.log', filemode='w',
                    format='%(asctime)s %(levelname)s %(message)s')

def create_consumer (config):
    #Create a kafka consumer with this details
    try: 
        consumer = Consumer(config)
        # print("Consumer created successfully")
        logging.info("Consumer created successfully")
        return consumer
    except KafkaException as e:
        # print(f"Failed to create consumer: {e}")
        logging.error(f"Failed to create consumer: {e}")
        raise
        sys.exit(1)

def subscribe_consumer(consumer, topics):
    #Subscribe the consumer to the given topics

    try: 
        consumer.subscribe(topics)
        print(f"Subscibed to topics: {topics}")
    except KafkaException as e:
        print(f"Failed to subscribe to topics {topics}: {e}")
        sys.exit(1)

def process_message(msg):
    print("Processing message") # Print statement for testing purpose
    #Process and save the message to a JSON file
    try: 
        #Convert message to a python dictionary
        message_data = json.loads(msg.value().decode('utf-8'))

        # Append the message to the JSON file
        with open("Kafka_messages.json", "a") as file:
            json.dump(message_data, file)
            file.write("\n") 

    except json.JSONDecodeError:
        print("Error: Received message is not a valid JSON")

def poll_messages(consumer):
    #poll messages continuosly from the kafka topic
    message_count = 0 # initialize the message counter

    try:
        while message_count < 10:
            print("Polling for message....")
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                print("No message recieved.")
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            print(f"Received message: {msg.value().decode('utf-8')}")
            process_message(msg)
            message_count +=1
    except KeyboardInterrupt:
        print("\nConsumer closed.")
    finally:
        consumer.close()

def main():
    #kafka configuration
    config = {
        'bootstrap.servers': '10.24.36.25:35002', 
        'group.id': 'pull-eventuat',
        'auto.offset.reset': 'earliest',
        'debug': 'all'
    }
    # Kafka topic to subscribe to
    topics = ['pull-eventuat']

    # Create kafka consumer
    consumer = create_consumer(config)

    # Subscribe to topics
    subscribe_consumer(consumer, topics)

    # Poll_messages(consumer)

if __name__ == "__main__":
    main()