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


TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getBreadCrumbData"
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
        def get_date_str():
            return datetime.now(tz=pytz.utc).astimezone(pytz.timezone("US/Pacific")).strftime("%Y-%m-%d")
        filename = f"{DATASTORE_PATH}{get_date_str()}.json"
        print(filename)
        data = json.dumps(data_as_dict, indent=4)

        with open(filename, 'w') as writer:
            writer.write(data)

        send_slack_notification(f"Today's Trimet data was retrieved and stored into {filename}")

    def fetch_data():
        try: 
            data = requests.get(TRIMET_DATA_URL).text
            data = json.loads(data)
            return data
        except:
            send_slack_notification("Today's data could not be fetched.")
            return None

    def parse_json(filename):
        with open(filename, "r") as read_file:
            data = json.load(read_file)
        return data

    def produce_data(data_list):
        length = len(data_list)
        print(length)
        bar = tqdm(total=length)
        for i in range(length):
            if(i % 10000 == 0):
                producer.flush()
            bar.update(1)
            data = json.dumps(data_list[i])
            # print("Data:", data)
            producer.produce(topic, value=data, callback=delivery_callback)
        producer.flush()
        send_slack_notification("Data was produced to the Kafka topic.")


    # Produce data by selecting random values from these lists.
    topic = "breadcrumbs_readings"

    if(args.data_filename):
        data_parsed = parse_json(args.data_filename)
        produce_data(data_parsed)
    else:
        print("Fetching data...")
        data = fetch_data()
        if(data):
            print("Saving data to json")
            save_to_json(data)
            produce_data(data)





