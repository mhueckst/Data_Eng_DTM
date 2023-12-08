#!/usr/bin/env python
"""
This script fetches stop event data, either from a provided file or by scraping,
transforms it into a suitable format, and then produces it to a Kafka topic.
"""

import json
import pandas as pd
from configparser import ConfigParser
from confluent_kafka import Producer
from argparse import ArgumentParser, FileType
from utilities import get_date_str
from send_slack_msg import send_slack_notification
import scrape
from tqdm import tqdm

# Constants
TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getStopEvents"
DATASTORE_PATH = "/home/consumed_data/"
TOPIC = "stop_data_readings"

def parse_arguments():
    """ Parses command-line arguments. """
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--data_filename', type=str, required=False)
    return parser.parse_args()

def initialize_producer(config_file):
    """ Initializes and returns a Kafka producer using the given configuration file. """
    config_parser = ConfigParser()
    config_parser.read_file(config_file)
    config = dict(config_parser['default'])
    return Producer(config)

def delivery_callback(err, msg):
    """ Handles the delivery status of a Kafka message. """
    if err:
        print(f'ERROR: Message failed delivery: {err}')

def save_to_json(data_as_dict, filename):
    """ Saves the given data as JSON to the specified file. """
    data = json.dumps(data_as_dict, indent=4)
    with open(filename, 'w') as writer:
        writer.write(data)
    send_slack_notification(f"Today's Trimet data was stored into {filename}")

def fetch_data():
    """ Fetches and returns stop event data. """
    try: 
        return scrape.transform_trip_df()
    except Exception as e:
        send_slack_notification(f"Today's stop event data could not be fetched: {e}")
        return None

def produce_data(producer, data_df, topic):
    """ Produces the given data frame to the specified Kafka topic. """
    length = len(data_df)
    if length == 0:
        print("Stop event data not available for this day")
        send_slack_notification("There is no stop event data to produce for this day")
        return
    data_df["json"] = data_df.apply(lambda x: x.to_json(), axis=1)
    bar = tqdm(total=length)
    for i, row in data_df.iterrows():
        if i % 10000 == 0:
            producer.flush()
        bar.update(1)
        producer.produce(topic, value=row["json"], callback=delivery_callback)
    producer.flush()
    send_slack_notification("Stop event data was produced to the Kafka topic.")

def main():
    args = parse_arguments()
    producer = initialize_producer(args.config_file)

    if args.data_filename:
        print("Retrieving data from file...")
        data_df = pd.read_csv(args.data_filename)
    else:
        print("Fetching data...")
        data_df = fetch_data()

    if data_df is not None:
        print("Producing data...")
        produce_data(producer, data_df, TOPIC)

if __name__ == '__main__':
    main()

