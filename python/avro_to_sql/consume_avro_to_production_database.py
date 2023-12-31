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
        'group.id': 'streaming_db',
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': value_deserializer
    }

    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe(['table-update'])

    # Database configuration
    conn_str = (
        "Driver={SQL Server};"
        "Server=10.24.37.99;"
        "Database=DATA_STREAMING_DB;"
        "Trusted_Connection=yes;"
        "Auto_Commit=true;"
    )
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()

    try:
        while True: 
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            # Convert message to Json
            message_json ={
                "key": msg.key(),
                "value": msg.value(),
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset()
            }

            # Insert Json data into SQL Server
            json_data = json.dumps(message_json, default=str)
            insert_query = "INSERT INTO Data_Event (JsonData) VALUES (?)"
            cursor.execute(insert_query, json_data)
            cnxn.commit()

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