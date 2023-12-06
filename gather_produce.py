#!/usr/bin/env python
import requests
import pytz
from datetime import datetime
from send_slack_msg import send_slack_notification
import sys
import json
from random import choice
from tqdm import tqdm
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from utilities import get_date_str


TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getBreadCrumbData"
#TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getStopEvents"
DATASTORE_PATH = "/home/dtm-project/data-archive/"

if __name__ == '__main__':

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--data_filename', type=str, required=False)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))


    def save_to_json(data_as_dict):
        filename = f"{DATASTORE_PATH}{get_date_str()}.json"
        print(filename)
        data = json.dumps(data_as_dict, indent=4)

        with open(filename, 'w') as writer:
            writer.write(data)

        send_slack_notification(f"Today's Trimet data was retrieved and stored into {filename}")

    def fetch_data():
        try: 
            data = requests.get(TRIMET_DATA_URL).text
            return data
        except:
            send_slack_notification("Today's data could not be fetched.")
            return None

    def parse_json(filename):
        with open(filename, "r") as read_file:
            data = read_file.read()
        # If little data was given (likely an error message)
        if(len(data) < 1000):
            data = []
        else:
            data = json.loads(data)
        return data

    def produce_data(data_list):
        length = len(data_list)
        if(length == 0):
            print("Data not available for this day")
            send_slack_notification("There is no data to produce for this day")
            return
        bar = tqdm(total=length)
        for i in range(length):
            if(i % 10000 == 0):
                producer.flush()
            bar.update(1)
            data = json.dumps(data_list[i])
            producer.produce(topic, value=data, callback=delivery_callback)
        producer.flush()
        send_slack_notification("Data was produced to the Kafka topic.")


    # Produce data by selecting random values from these lists.
    topic = "breadcrumbs_readings"

    if(args.data_filename):
        print("Retrieving data from file...")
        data_parsed = parse_json(args.data_filename)
        produce_data(data_parsed)
    else:
        print("Fetching data...")
        data = fetch_data()
        # If little data was given (likely an error message)
        if(len(data) < 1000):
            data = []
        elif(data):
            data = json.loads(data)
            print("Producing data...")
            produce_data(data)
