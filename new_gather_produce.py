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
import scrape
import pandas as pd

#TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getBreadCrumbData"
TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getStopEvents"
DATASTORE_PATH = "/home/dtm-project/consumed_data/"

if __name__ == '__main__':

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--data_filename', type=str, required=False)
    args = parser.parse_args()


    # Produce data by selecting random values from these lists.
    topic = "stop_data_readings"

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


    # calls scrape.py to scrape Stop data from Trimet website. Stores to csv and returns file path
    def fetch_data():
        try: 
            data_df = scrape.transform_trip_df()
            return data_df
        except:
            send_slack_notification("Today's stop event data could not be fetched.")
            return None

    def produce_data(data_df):
        length = len(data_df)
        if(length == 0):
            print("Stop event data not available for this day")
            send_slack_notification("There is no stop event data to produce for this day")
            return
        bar = tqdm(total=length)
        data_df["json"] = data_df.apply(lambda x: x.to_json(), axis=1)
        for i in range(length):
            if(i % 10000 == 0):
                producer.flush()
            bar.update(1)
            data = data_df.iloc[i]["json"]
            # print("Data:", data)
            producer.produce(topic, value=data, callback=delivery_callback)
        producer.flush()
        send_slack_notification("Stop event data was produced to the Kafka topic.")

    data_df = None
    # If a cmd argument was provided, read from csv and store into a dataframe.
    if(args.data_filename):
        print("Retrieving data from file...")
        data_df = pd.read_csv(args.data_filename)
    # If no args provided, fetch today's stop event data and store into a dataframe.
    else:
        print("Fetching data...")
        data_df = fetch_data()
        print(data_df)
        print("Producing data...")
    # Produce the data in the dataframe row by row
    produce_data(data_df)
