from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

def main():
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

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print(f"Consumed record with key {msg.key()} and value {msg.value()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
