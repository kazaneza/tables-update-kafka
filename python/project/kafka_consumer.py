from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

def setup_kafka_consumer():
    schema_registry_conf = {'url': 'http://10.24.36.25:35003'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)

    consumer_config = {
        'bootstrap.servers': '10.24.36.25:35002',
        'group.id': 'test07/01/20241',
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': value_deserializer
    }

    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe(['table-update'])
    return consumer

def poll_message(consumer):
    return consumer.poll(1.0)