import json
import time
from datetime import datetime, date
import pytz  # For timezone handling
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import os
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
# !!! IMPORTANT: Replace 'localhost:9092' with your actual Kafka broker(s) address !!!
KAFKA_BOOTSTRAP_SERVERS = [os.environ.get('KAFKA_BOOTSTRAP_SERVERS','hourly_topic')]
NY_TIMEZONE_STR = 'America/New_York'

HOURLY_PREDICTION_TOPIC = os.environ.get('KAFKA_HOURLY_PREDICTION_TOPIC','hourly_topic')
DAILY_TRAIN_TOPIC = os.environ.get('KAFKA_DAILY_TRAIN_TOPIC','daily_topic')

# State variables to track when messages were last sent
# For hourly triggers: {(hour, minute): date_object_when_sent}
_last_sent_hourly_trigger = {}
# For daily trigger: date_object_when_sent
_last_sent_daily_train_trigger_date = None
# --- End Configuration ---

def create_kafka_producer(bootstrap_servers):
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka-trigger-scheduler-py',
            # For production, consider adding:
            # retries=5,
            # acks='all', # For stronger delivery guarantees
            # value_serializer=lambda v: json.dumps(v).encode('utf-8') # If all messages are JSON
        )
        print(f"Successfully connected to Kafka brokers at {bootstrap_servers}")
        return producer
    except NoBrokersAvailable:
        print(f"FATAL: No Kafka brokers available at {bootstrap_servers}. Please check your Kafka setup and KAFKA_BOOTSTRAP_SERVERS setting.")
        return None
    except Exception as e:
        print(f"FATAL: Could not create Kafka producer. Error: {e}")
        return None

def send_kafka_message(producer: KafkaProducer, topic: str, message_data: dict):
    """Sends a JSON message to a Kafka topic."""
    if not producer:
        print(f"Kafka producer is not available. Cannot send message to topic '{topic}'.")
        return False
    try:
        # Encode message to JSON string, then to bytes
        future = producer.send(topic, json.dumps(message_data).encode('utf-8'))
        # Block for 'synchronous' sends with a timeout
        record_metadata = future.get(timeout=10)
        # print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
        print(f"Successfully sent message to Kafka topic '{topic}': {message_data['trigger_type']} at {message_data['scheduled_time_ny_friendly']}")
        return True
    except KafkaError as e:
        print(f"Error sending message to Kafka topic '{topic}': {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while sending Kafka message to '{topic}': {e}")
        return False

def run_kafka_triggers_scheduler():
    global _last_sent_hourly_trigger, _last_sent_daily_train_trigger_date

    print(f"Kafka Trigger Scheduler started for New York Time ({NY_TIMEZONE_STR}).")
    print(f"Attempting to connect to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")

    kafka_producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    if not kafka_producer:
        print("Exiting due to Kafka connection failure.")
        return

    # Define target times
    target_hourly_times_tuples = [
        (9, 30), (10, 30), (11, 30), (12, 30),  # AM/PM
        (13, 30), (14, 30), (15, 30), (16, 30)   # PM
    ]
    target_daily_train_time_tuple = (8, 0)  # 8:00 AM

    try:
        while True:
            try:
                ny_timezone = pytz.timezone(NY_TIMEZONE_STR)
                now_ny = datetime.now(ny_timezone)
                current_date_ny = now_ny.date()
                current_hour_ny = now_ny.hour
                current_minute_ny = now_ny.minute
            except pytz.exceptions.UnknownTimeZoneError:
                print(f"ERROR: Timezone '{NY_TIMEZONE_STR}' not found. Ensure 'pytz' is installed correctly.")
                time.sleep(300) # Wait 5 minutes before retrying timezone loading
                continue
            except Exception as e:
                print(f"ERROR getting current time: {e}. Retrying in 60 seconds.")
                time.sleep(60)
                continue

            # --- 1. Handle Hourly Prediction Triggers ---
            for target_hour, target_minute in target_hourly_times_tuples:
                if current_hour_ny == target_hour and current_minute_ny == target_minute:
                    slot_key = (target_hour, target_minute)
                    if _last_sent_hourly_trigger.get(slot_key) != current_date_ny:
                        scheduled_time_obj = datetime(current_date_ny.year, current_date_ny.month, current_date_ny.day,
                                                      target_hour, target_minute, tzinfo=ny_timezone)
                        message_payload = {
                            "trigger_type": "hourly_prediction",
                            "event_timestamp_utc": datetime.utcnow().isoformat() + "Z",
                            "scheduled_time_ny_iso": scheduled_time_obj.isoformat(),
                            "scheduled_time_ny_friendly": scheduled_time_obj.strftime("%I:%M %p")
                        }
                        if send_kafka_message(kafka_producer, HOURLY_PREDICTION_TOPIC, message_payload):
                            _last_sent_hourly_trigger[slot_key] = current_date_ny
                        # Break because we only send one type of message per minute check
                        # and this hourly trigger has precedence for this minute if it matches.
                        break 

            # --- 2. Handle Daily Train Trigger ---
            if (current_hour_ny == target_daily_train_time_tuple[0] and
                    current_minute_ny == target_daily_train_time_tuple[1]):
                if _last_sent_daily_train_trigger_date != current_date_ny:
                    scheduled_time_obj = datetime(current_date_ny.year, current_date_ny.month, current_date_ny.day,
                                                  target_daily_train_time_tuple[0], target_daily_train_time_tuple[1],
                                                  tzinfo=ny_timezone)
                    message_payload = {
                        "trigger_type": "daily_train",
                        "event_timestamp_utc": datetime.utcnow().isoformat() + "Z",
                        "scheduled_time_ny_iso": scheduled_time_obj.isoformat(),
                        "scheduled_time_ny_friendly": scheduled_time_obj.strftime("%I:%M %p")
                    }
                    if send_kafka_message(kafka_producer, DAILY_TRAIN_TOPIC, message_payload):
                        _last_sent_daily_train_trigger_date = current_date_ny
            
            # Sleep efficiently until the start of the next minute
            seconds_into_current_minute = now_ny.second + (now_ny.microsecond / 1_000_000.0)
            sleep_duration = 60.0 - seconds_into_current_minute + 0.05 #
            
            if sleep_duration <= 0: # Should only happen if processing took > 60s
                 sleep_duration = 1.0 # Avoid negative sleep, ensure loop continues
            
            # print(f"DEBUG: Sleeping for {sleep_duration:.2f}s. Current NY Time: {now_ny.strftime('%H:%M:%S')}")
            time.sleep(sleep_duration)

    except KeyboardInterrupt:
        print("\nScheduler stopped by user (KeyboardInterrupt).")
    except Exception as e:
        print(f"An critical unexpected error occurred in the main scheduler loop: {e}")
    finally:
        if kafka_producer:
            print("Closing Kafka producer...")
            kafka_producer.close()
            print("Kafka producer closed.")

if __name__ == '__main__':
    run_kafka_triggers_scheduler()