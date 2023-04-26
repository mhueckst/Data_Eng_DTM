#path = "/home/dtm-project/consumer_records.txt"

import sys
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from argparse import ArgumentParser, FileType
from utilities import get_date_str
from send_slack_msg import send_slack_notification

path = f"/home/dtm-project/consumed_data/{get_date_str()}.txt"

# Parse the command line.
parser = ArgumentParser()
# parser.add_argument('config_file', type=FileType('r'))
parser.add_argument('--reset', action='store_true')
# parser.add_argument('output_file', type=str, help='The file to save incoming records')
args = parser.parse_args()

# Parse the configuration.
config_parser = ConfigParser()
with open("./getting_started.ini", "r") as config_file:

    # config_parser.read_file(args.config_file)
    config_parser.read_file(config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

# Create Consumer instance
consumer = Consumer(config)

# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

# Subscribe to topic
topic = "breadcrumbs_readings"
consumer.subscribe([topic], on_assign=reset_offset)

started_consuming = False

# Open the output file for writing.
# with open(args.output_file, 'w') as output_file:
with open(path, 'a') as output_file:

    # Poll for new messages from Kafka and write them to the file.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
                started_consuming = False
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                if(started_consuming == False):
                    started_consuming = True
                    print("Data is being consumed.")
                    send_slack_notification("Data is being consumed.")
                key = msg.key().decode('utf-8') if msg.key() is not None else None
                value = msg.value().decode('utf-8') if msg.value() is not None else None

                print("Consumed event from topic {topic}: key = {key} value = {value}".format(
                topic=msg.topic(), key=key if key is not None else 'None', value=value if value is not None else 'None'))

                output_file.write(f"{value}\n")
    except:
        print("\nConsumer stopped.")
        output_file.close() 
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
