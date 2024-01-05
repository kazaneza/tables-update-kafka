from kafka_consumer import setup_kafka_consumer, poll_message
from db_operations import get_db_connection, execute_insert_query
from message_handler import process_message
import json

# Load config
with open('config.json', 'r') as file:
    config = json.load(file)

def main():
    consumer = setup_kafka_consumer()
    cnxn = get_db_connection()
    cursor = cnxn.cursor()

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None or msg.error():
                continue

            json_data, processing_time, entity_name, entity_id = process_message(msg)

            if entity_name in config:
                table_config = config[entity_name]
                table_name = table_config["table"]
                columns = ', '.join(table_config["columns"])
                placeholders = ', '.join(['?'] * len(table_config["columns"]))
                insert_query = f"INSERT INTO {table_name}({columns}) VALUES({placeholders})"
                values = [json_data, processing_time, entity_name, entity_id]

                execute_insert_query(cursor, insert_query, *values)
                cnxn.commit()

            

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        cursor.close()
        cnxn.close()

if __name__ == '__main__':
    main()
