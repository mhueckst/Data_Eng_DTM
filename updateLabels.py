import json
import sys

# Global variables for source and destination filenames
source_filename = None
destination_filename = None

def retrieve_args():
    """
    Retrieves command-line arguments and sets global variables for source and destination filenames.

    Raises:
        Exception: If the required command-line arguments are not provided.
    """
    global source_filename, destination_filename

    if len(sys.argv) < 2:
        raise Exception("Please provide a source filename as the first command-line argument.")
    if len(sys.argv) < 3:
        raise Exception("Please provide a destination filename as the second command-line argument.")

    source_filename = sys.argv[1]
    destination_filename = sys.argv[2]

def replace_labels(data):
    """
    Replaces specific string patterns in the provided data string.

    The replacements are done in a specific order to ensure correct data manipulation.

    :param data: String data read from the source file.
    :return: Modified data with replaced labels.
    """
    print("Updating labels...")
    # GPS_LONGITUDE must be replaced before VELOCITY to avoid data corruption.
    data = data.replace("GPS_LONGITUDE", "GPS_HDOP")  # Must come before
    data = data.replace("VELOCITY", "GPS_LONGITUDE")  # Must come after
    data = data.replace("DIRECTION", "GPS_LATITUDE")
    data = data.replace("RADIO_QUALITY", "GPS_SATELLITES")
    return data

def write_to_file(data, destination):
    """
    Writes the given data to the specified destination file.

    :param data: String data to be written to the file.
    :param destination: Destination filename to write the data.
    """
    print("Writing to file...")
    with open(destination, 'w') as writer:
        writer.write(data)

if __name__ == "__main__":
    try:
        retrieve_args()
        print("Loading data from file...")
        with open(source_filename, 'r') as reader:
            data = reader.read()

        # Replace all incorrect labels
        updated_data = replace_labels(data)

        # Write the modified data to the destination file
        write_to_file(updated_data, destination_filename)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

