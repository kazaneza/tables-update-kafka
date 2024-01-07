from kafka_consumer import setup_kafka_consumer, poll_message
from db_operations import get_db_connection, execute_insert_query
from message_handler import process_message
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def main():
    start_time = datetime.now()
    logging.info("Starting main process")
    with open('config.json', 'r') as file:
        config = json.load(file)

    consumer = setup_kafka_consumer()
    cnxn = get_db_connection()
    cursor = cnxn.cursor()

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None or msg.error():
                continue

            # logging.info("Message received")
            message_json, message_fields = process_message(msg, config)
            entity_name = message_fields.get('entityName')

            if entity_name in config:
                table_config = config[entity_name]
                table_name = table_config["table"]
                columns = table_config["columns"]

                column_names = ', '.join(columns)
                placeholders = ', '.join(['?'] * len(columns))
                insert_query = f"INSERT INTO {table_name}({column_names}) VALUES({placeholders})"

                values = [message_json] + [message_fields.get(column) for column in columns[1:]]
                execute_insert_query(cursor, insert_query, values)
                cnxn.commit()
                # logging.info(f"Data inserted into {table_name}")

    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        end_time = datetime.now()
        duration = end_time -start_time
        logging.info(f"Closing resources. Session duration: {duration}")
        consumer.close()
        cursor.close()
        cnxn.close()

if __name__ == '__main__':
    main()