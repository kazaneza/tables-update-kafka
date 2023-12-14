import json
import pyodbc
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

def main():
    # Kafka configuration
    schema_registry_conf = {'url': 'http://10.24.36.25:35003'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)

    consumer_config = {
        'bootstrap.servers': '10.24.36.25:35002',
        'group.id': 'table-update',
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': value_deserializer
    }

    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe(['table-update'])

    # Database configuration
    conn_str = (
        "Driver={SQL Server};"
        "Server=localhost;"
        "Database=KafkaMessageJson;"
        "Trusted_Connection=yes;"
    )
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()

    try:
        message_count = 0
        while message_count < 3:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            # Convert message to JSON
            message_json = {
                "key": msg.key(),
                "value": msg.value(),
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset()
            }

            # Insert JSON data into SQL Server
            json_data = json.dumps(message_json, default=str)
            insert_query = "INSERT INTO KafkaMessages (JsonData) VALUES (?)"
            cursor.execute(insert_query, json_data)
            cnxn.commit()

            message_count += 1
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the consumer and database connection
        consumer.close()
        cursor.close()
        cnxn.close()

if __name__ == '__main__':
    main()
