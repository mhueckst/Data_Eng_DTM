"""
This script consumes messages from a Kafka topic and writes them to a file.
It supports resetting the Kafka offset and specifies the output file path.
"""
# Alternative file path for storing consumer records (currently not in use)
# path = "/home/dtm-project/consumer_records.txt"

import sys
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING
from argparse import ArgumentParser
from utilities import get_date_str
from send_slack_msg import send_slack_notification

def parse_arguments():
    """ Parses command-line arguments """
    parser = ArgumentParser()
    parser.add_argument('--reset', action='store_true')
    return parser.parse_args()

def parse_config():
    """ Parses configuration from 'getting_started.ini' file """
    config_parser = ConfigParser()
    with open("./getting_started.ini", "r") as config_file:
        config_parser.read_file(config_file)
        config = dict(config_parser['default'])
        config.update(config_parser['consumer'])
    return config

def reset_offset(consumer, partitions):
    """ Resets the Kafka offset if the '--reset' flag is provided """
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

def consume_messages(consumer, output_path):
    """ Consumes messages from Kafka and writes them to a file """
    with open(output_path, 'a') as output_file:
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    print("Waiting...")
                elif msg.error():
                    raise KafkaError(msg.error())
                else:
                    process_message(msg, output_file)
        except KafkaError as e:
            print(f"Kafka error: {e}")
        finally:
            consumer.close()

def process_message(msg, output_file):
    """ Processes a single Kafka message """
    key = msg.key().decode('utf-8') if msg.key() is not None else None
    value = msg.value().decode('utf-8') if msg.value() is not None else None
    print(f"Consumed event: key = {key or 'None'} value = {value or 'None'}")
    output_file.write(f"{value}\n")

if __name__ == "__main__":
    args = parse_arguments()
    config = parse_config()
    consumer = Consumer(config)
    consumer.subscribe(["breadcrumbs_readings"], on_assign=reset_offset)
    output_file_path = f"/home/dtm-project/consumed_data/{get_date_str()}.txt"
    consume_messages(consumer, output_file_path)
    