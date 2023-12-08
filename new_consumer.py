#!/usr/bin/env python
"""
This script consumes Kafka messages from the 'stop_data_readings' topic,
converts them into CSV format, and writes to a file. It also handles
resetting offsets and tracks any conversion failures.
"""

import sys
import json
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from argparse import ArgumentParser
from utilities import get_date_str
import os
# from send_slack_msg import send_slack_notification

def parse_arguments():
    """
    Parses command-line arguments and returns the parsed arguments.
    """
    parser = ArgumentParser()
    parser.add_argument('config_file', type=str, help='Configuration file for the Kafka consumer.')
    parser.add_argument("--reset", action="store_true", help="Reset the Kafka offset if provided.")
    return parser.parse_args()

def initialize_consumer(config_file_path):
    """
    Initializes and returns a Kafka Consumer with configuration from the given file.
    """
    config_parser = ConfigParser()
    with open(config_file_path, "r") as config_file:
        config_parser.read_file(config_file)
        config = dict(config_parser["default"])
        config.update(config_parser["consumer"])
    return Consumer(config)

def json_to_csv(json_string, keys):
    """
    Converts a JSON string to a CSV format string based on specified keys.
    """
    try:
        obj = json.loads(json_string)
        return ",".join(str(obj.get(key, "")) for key in keys) + "\n"
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return ""

def consume_and_write_to_csv(consumer, output_path, topic, keys):
    """
    Consumes messages from Kafka and writes them to a CSV file.
    """
    with open(output_path, "w") as output_file:
        output_file.write(",".join(keys) + "\n")
        started_consuming = False
        number_of_fails = 0

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
                continue
            if msg.error():
                print(f"ERROR: {msg.error()}")
                continue

            if not started_consuming:
                started_consuming = True
                print("Stop event data is being consumed.")
                # send_slack_notification("Data is being consumed.")

            csv_string = json_to_csv(msg.value().decode("utf-8"), keys)
            if csv_string:
                output_file.write(csv_string)
            else:
                number_of_fails += 1

    print(f"\nStop event consumer stopped. Number of fails: {number_of_fails}")

def reset_offset(consumer, partitions, reset):
    """
    Resets the offset for the consumer if the reset flag is set.
    """
    if reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

def main():
    args = parse_arguments()

    consumer = initialize_consumer(args.config_file)
    topic = "stop_data_readings"
    keys = ["trip_id", "route_number", "service_key", "direction"]

    output_path = f"/home/consumed_data/trips_{get_date_str()}.csv"

    # Create the directory if it does not exist
    directory = os.path.dirname(output_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    consumer.subscribe([topic], on_assign=lambda c, p: reset_offset(c, p, args.reset))
    consume_and_write_to_csv(consumer, output_path, topic, keys)

if __name__ == "__main__":
    main()


