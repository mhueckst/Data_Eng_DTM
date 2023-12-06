import json
import sys

source_filename = None
destination_filename = None

# Retrieves cmd arguments
def retrieve_args():
    global source_filename, destination_filename
    if(len(sys.argv) < 2):
        raise Exception ("Please provide a source filename as first cmd argument.")
    if(len(sys.argv) < 3):
        raise Exception ("Please provide a destination filename as the second cmd argument.")
    source_filename = sys.argv[1]
    destination_filename = sys.argv[2]

if(__name__ == "__main__"):
    retrieve_args()

    # Load the old text file as one giant string
    print("Loading data from file...")
    with open(source_filename, 'r') as reader:
        data = reader.read()

    # Replace all incorrect labels
    print("Updating labels...")
    # IMPORTANT!!!
    # GPS_LONGITUDE must be replaced with GPS_HDOP before VELOCITY is
    # replaced with GPS_LONGITUDE. Otherwise, we'll be overwriting VELOCITY
    # with GPS_HDOP as well.
    data = data.replace("GPS_LONGITUDE", "GPS_HDOP") # Must come before
    data = data.replace("VELOCITY", "GPS_LONGITUDE") # Must come after
    data = data.replace("DIRECTION", "GPS_LATITUDE")
    data = data.replace("RADIO_QUALITY", "GPS_SATELLITES")

    # Write the new giant string to a file
    print("Writing to file...")
    with open(destination_filename, 'w') as writer:
        writer.write(data)
