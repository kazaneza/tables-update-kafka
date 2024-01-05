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

            message_json, message_fields = process_message(msg)
            entity_name = message_fields.get('entityName')

            if entity_name in config:
                table_config = config[entity_name]
                table_name = table_config["table"]
                # columns = ', '.join(table_config["columns"])
                columns = table_config["columns"]

                # Construct the query
                column_names = ', '.join(columns)
                placeholders = ', '.join(['?'] * len(columns))
                insert_query = f"INSERT INTO {table_name}({column_names}) VALUES({placeholders})"

                # Prepare the values for insertion
                values = [message_json] + [message_fields.get(column) for column in columns[1:]]

                # Execute the query
                execute_insert_query(cursor, insert_query, values)
                cnxn.commit()
                


                # placeholders = ', '.join(['?'] * len(table_config["columns"]))
                # insert_query = f"INSERT INTO {table_name}({columns}) VALUES({placeholders})"
                # values = [json_data, processing_time, entity_name, entity_id]

            

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
