#!/usr/bin/env python

"""
This module is responsible for fetching, storing, and producing Trimet data to a Kafka topic.
It supports reading from a configuration file and optional command-line arguments to control data flow.
"""

import requests
import json
from datetime import datetime
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from utilities import get_date_str
from send_slack_msg import send_slack_notification

TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getBreadCrumbData"
DATASTORE_PATH = "/home/data-archive/"

def parse_arguments():
    """
    Parses command-line arguments and returns the parsed arguments.
    """
    parser = ArgumentParser(description="Fetch and produce Trimet data to Kafka.")
    parser.add_argument('config_file', type=FileType('r'), help='Configuration file for the Kafka producer.')
    parser.add_argument('--data_filename', type=str, help='Optional: Specify a file to read the data from.')
    return parser.parse_args()

def parse_config(file):
    """
    Parses the given configuration file for Kafka producer settings.
    Returns a dictionary of the configuration.
    """
    config_parser = ConfigParser()
    config_parser.read_file(file)
    return dict(config_parser['default'])

def delivery_callback(err, msg):
    """
    Callback to handle message delivery status.
    Logs an error if message delivery fails.
    """
    if err:
        print(f'ERROR: Message failed delivery: {err}')

def save_to_json(data_as_dict, filename):
    """
    Saves the given data to a JSON file.
    Notifies via Slack after successful storage.
    """
    with open(filename, 'w') as writer:
        writer.write(json.dumps(data_as_dict, indent=4))
    send_slack_notification(f"Today's Trimet data was retrieved and stored into {filename}")

def fetch_data():
    """
    Fetches data from the Trimet data URL.
    Returns the data as text or None if an error occurs.
    """
    try: 
        return requests.get(TRIMET_DATA_URL).text
    except requests.RequestException as e:
        send_slack_notification("Today's data could not be fetched. Error: " + str(e))
        return None

def parse_json(data):
    """
    Parses the given JSON-formatted data.
    Returns a list of data if JSON is valid, or an empty list if data is too small.
    """
    if len(data) < 1000:
        return []
    return json.loads(data)

def produce_data(producer, data_list, topic):
    """
    Produces data to the Kafka topic.
    Notifies via Slack upon completion.
    """
    if not data_list:
        print("Data not available for this day")
        send_slack_notification("There is no data to produce for this day")
        return
    
    for i, record in enumerate(data_list):
        if i % 10000 == 0:
            producer.flush()
        producer.produce(topic, value=json.dumps(record), callback=delivery_callback)
    producer.flush()
    send_slack_notification("Data was produced to the Kafka topic.")

if __name__ == '__main__':
    args = parse_arguments()
    config = parse_config(args.config_file)
    producer = Producer(config)

    topic = "breadcrumbs_readings"
    data_filename = args.data_filename

    if data_filename:
        print("Retrieving data from file...")
        with open(data_filename, "r") as file:
            data_parsed = parse_json(file.read())
        produce_data(producer, data_parsed, topic)
    else:
        print("Fetching data...")
        data = fetch_data()
        if data:
            data_parsed = parse_json(data)
            if data_parsed:
                print("Producing data...")
                produce_data(producer, data_parsed, topic)
            else:
                print("No valid data to produce.")

