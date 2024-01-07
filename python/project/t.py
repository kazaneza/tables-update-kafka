import sys

# ... [rest of your imports] ...

def main():
    start_time = datetime.now()
    message_count = 0
    logging.info("Starting main process")

    # ... [rest of your setup code] ...

    try:
        while True:
            msg = poll_message(consumer)
            if msg is None or msg.error():
                continue

            # Process the message
            # ... [rest of your message processing code] ...

            # Increment and display the message counter
            message_count += 1
            sys.stdout.write(f"\rMessages processed: {message_count} /n")
            sys.stdout.flush()

            # ... [rest of your loop code] ...

    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"\nClosing resources. Session duration: {duration}")
        consumer.close()
        cursor.close()
        cnxn.close()

if __name__ == '__main__':
    main()
