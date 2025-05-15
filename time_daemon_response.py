import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import os
from dotenv import load_dotenv
load_dotenv()

# --- Import your custom routines ---
# Ensure 'my_routines.py' is in the same directory or Python's path
try:
    import core_routines as my_routines
except ImportError:
    print("ERROR: Could not import 'my_routines.py'. Make sure the file exists and is accessible.")
    exit(1)

# --- Configuration ---
# !!! IMPORTANT: Replace 'localhost:9092' with your actual Kafka broker(s) address !!!
KAFKA_BOOTSTRAP_SERVERS = [os.environ.get('KAFKA_BOOTSTRAP_SERVERS','hourly_topic')]
HOURLY_TOPIC = os.environ.get('KAFKA_HOURLY_PREDICTION_TOPIC','hourly_topic')
DAILY_TOPIC = os.environ.get('KAFKA_DAILY_TRAIN_TOPIC','daily_topic')
CONSUMER_GROUP_ID = 'my-trigger-consumer-group' # Define a consumer group ID
# --- End Configuration ---

def run_kafka_consumer():
    print(f"Starting Kafka Consumer for topics: ['{HOURLY_TOPIC}', '{DAILY_TOPIC}']")
    print(f"Connecting to Kafka brokers at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Consumer Group ID: {CONSUMER_GROUP_ID}")

    consumer = None
    while consumer is None: # Loop until Kafka connection is successful
        try:
            consumer = KafkaConsumer(
                HOURLY_TOPIC,
                DAILY_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',  # Process messages from the beginning if consumer is new or was down
                # auto_offset_reset='latest', # Or 'latest' to only process new messages after consumer starts
                group_id=CONSUMER_GROUP_ID,
                # value_deserializer=lambda m: json.loads(m.decode('utf-8')), # Optional: deserialize here
                consumer_timeout_ms=1000  # Timeout for polling, to allow KeyboardInterrupt
            )
            print("Successfully connected to Kafka and subscribed to topics.")
        except NoBrokersAvailable:
            print(f"No Kafka brokers available at {KAFKA_BOOTSTRAP_SERVERS}. Retrying in 15 seconds...")
            time.sleep(15)
        except Exception as e:
            print(f"An unexpected error occurred while connecting to Kafka: {e}. Retrying in 15 seconds...")
            time.sleep(15)


    print("Listening for messages...")
    try:
        for message in consumer:
            print(f"\nReceived message:")
            print(f"  Topic: {message.topic}")
            print(f"  Partition: {message.partition}, Offset: {message.offset}")
            print(f"  Key: {message.key}")
            
            try:
                # Assuming the message value is a JSON string produced by the previous script
                message_value_str = message.value.decode('utf-8')
                message_data = json.loads(message_value_str)
                print(f"  Value (JSON): {message_data}")

                trigger_type = message_data.get("trigger_type")
                scheduled_time_friendly = message_data.get("scheduled_time_ny_friendly", "N/A")

                if message.topic == HOURLY_TOPIC:
                    print(f"  Action: Triggering per_hour_routine for scheduled time {scheduled_time_friendly}...")
                    try:
                        #print(f"TRIGGERED per_hour_routine")
                        my_routines.per_hour_routine()
                    except Exception as e:
                        print(f"  ERROR during per_hour_routine: {e}")
                elif message.topic == DAILY_TOPIC:
                    print(f"  Action: Triggering per_day_routine for scheduled time {scheduled_time_friendly}...")
                    try:
                        #print(f"TRIGGERED per_day_routine")
                        my_routines.per_day_routine()
                    except Exception as e:
                        print(f"  ERROR during per_day_routine: {e}")
                else:
                    print(f"  Warning: Received message from an unexpected topic: {message.topic}")

            except json.JSONDecodeError:
                print(f"  Error: Could not decode message value as JSON: {message.value}")
            except AttributeError:
                print(f"  Error: 'my_routines' module or its functions might not be loaded correctly.")
            except Exception as e:
                print(f"  An unexpected error occurred processing message: {e}")
            
            # Manually commit offset if auto_commit_enable is false (default is true for kafka-python)
            # consumer.commit() # Not strictly needed with default auto-commit settings

    except KeyboardInterrupt:
        print("\nConsumer stopped by user (KeyboardInterrupt).")
    except Exception as e:
        print(f"A critical error occurred in the consumer loop: {e}")
    finally:
        if consumer:
            print("Closing Kafka consumer...")
            consumer.close()
            print("Kafka consumer closed.")

if __name__ == '__main__':
    while True:
        run_kafka_consumer()