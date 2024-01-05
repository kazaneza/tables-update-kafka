import json 
import pyodbc
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

def consume_kafka_message():
    # Kafka configuration
    schema_registry_conf = {'url': 'http://10.24.36.25:35003' }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)

    consumer_config = {
        'bootstrap.servers': '10.24.36.25:35002',
        'group.id': 'test_28/12/2023',
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': value_deserializer
    }

    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe(['table-update'])

    # Database configuration
    conn_str = (
        "Driver={SQL Server};"
        "Server=ACADEMY06;"
        "Database=KafkaMessageJson;"
        "Trusted_Connection=yes;"
        "Auto_commit=true;"
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

            # Extract additional fields
            processing_time = msg.value().get('processingTime', None)
            entity_name = msg.value().get('entityName', None)
            entity_id = msg.value().get('entityId', None)

            # Prepare Json data
            json_data = json.dumps(message_json, default=str)

            # Update insert query with new columns
            insert_query = """
            INSERT INTO KafkaMessages (JsonData, ProcessingTime, EntityName, EntityId)
            VALUES (?, ?, ?, ?)
            """

            cursor.execute(insert_query, json_data, processing_time, entity_name, entity_id)
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
    consume_kafka_message()
