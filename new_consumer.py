# path = "/home/dtm-project/consumer_records.txt"

import sys
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from argparse import ArgumentParser, FileType
from utilities import get_date_str
from send_slack_msg import send_slack_notification
import json

path = f"/home/dtm-project/consumed_data/trips_{get_date_str()}.csv"
PRINT_CONSUMED_RECORDS = False
KEYS = [
    "trip_id",
    "route_number",
    "service_key",
    "direction"
]
LAST_KEY = KEYS[len(KEYS) - 1]
CSV_LABELS = ""
for key in KEYS:
    CSV_LABELS += key
    if key == LAST_KEY:
        CSV_LABELS += "\n"
    else:
        CSV_LABELS += ","

# Parse the command line.
parser = ArgumentParser()
# parser.add_argument('config_file', type=FileType('r'))
parser.add_argument("--reset", action="store_true")
# parser.add_argument('output_file', type=str, help='The file to save incoming records')
args = parser.parse_args()

# Parse the configuration.
config_parser = ConfigParser()
with open("./getting_started.ini", "r") as config_file:
    # config_parser.read_file(args.config_file)
    config_parser.read_file(config_file)
    config = dict(config_parser["default"])
    config.update(config_parser["consumer"])

# Create Consumer instance
consumer = Consumer(config)


# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


# Convert one JSON to string format
def json_to_csv(string):
    obj = json.loads(string)
    csv_string = ""

    for key in KEYS:
        value = ""
        if key in obj:
            value = str(obj[key])
        csv_string += value
        if key == LAST_KEY:
            csv_string += "\n"
        else:
            csv_string += ","

    return csv_string


# Subscribe to topic
topic = "stop_data_readings"
consumer.subscribe([topic], on_assign=reset_offset)

started_consuming = False
number_of_fails = 0
# Open the output file for writing.
# with open(args.output_file, 'w') as output_file:
with open(path, "w") as output_file:
    # Poll for new messages from Kafka and write them to the file.
    try:
        output_file.write(CSV_LABELS)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
                started_consuming = False
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                if started_consuming == False:
                    started_consuming = True
                    print("Stop event data is being consumed.")
                    # send_slack_notification("Data is being consumed.")

                key = msg.key().decode("utf-8") if msg.key() is not None else None
                value = msg.value().decode("utf-8") if msg.value() is not None else None

                if PRINT_CONSUMED_RECORDS:
                    print(
                        "Consumed event from topic {topic}: key = {key} value = {value}".format(
                            topic=msg.topic(),
                            key=key if key is not None else "None",
                            value=value if value is not None else "None",
                        )
                    )
                try:
                    csv_string = json_to_csv(value)
                except Exception as e:
                    print(e)
                    number_of_fails += 1
                output_file.write(csv_string)
    except:
        print("\nStop event consumer stopped.")
        print(f"Number of fails: {number_of_fails}")
        output_file.close()
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
