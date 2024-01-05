from kafka_consumer import setup_kafka_consumer, poll_message
from db_operations import get_db_connection, execute_insert_query
from message_handler import process_message

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

            if entity_name == "FBNK.CUSTOMER":
                # Define your insert query for FBNK.CUSTOMER
                insert_query = """
                INSERT INTO FBNK_CUSTOMER(JsonData, ProcessingTime, EntityName, EntityId)
                VALUES(?,?,?,?)
                """
            else:
                # Existing insert query for other cases
                insert_query = """
                INSERT INTO KafkaMessages(JsonData, ProcessingTime, EntityName, EntityId)
                VALUES(?,?,?,?)
                """

            execute_insert_query(cursor, insert_query, json_data, processing_time, entity_name, entity_id)
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
