import pandas as pd
from datetime import datetime, timedelta
import sys, math
from send_slack_msg import send_slack_notification

def transform_csv(csv):
    """
    Transforms the CSV file data.
    """
    df = pd.read_csv(csv)
    # Drop columns only if they exist in the DataFrame
    columns_to_drop = ["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    return df

def validate_data(df):
    """
    Applies various validation checks to the DataFrame.
    """
    # Add specific validation logic here
    # Example: df = validate_some_aspect(df)
    # ...

    return df

def read_csv_and_validate_all(csv):
    """
    Reads, transforms, and validates the CSV file data.
    """
    df = transform_csv(csv)
    df = validate_data(df)
    return df

# You can define more specific transformation and validation functions here
# ...

if __name__ == "__main__":
    # Command line argument or input for CSV file path
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = input("CSV file to transform: ")

    if csv_file:
        try:
            # Processing the CSV file
            df = read_csv_and_validate_all(csv_file)
            # Further actions with the processed data
            print(df)
        except Exception as e:
            # Error handling
            send_slack_notification(f"An error occurred: {e}")
            print(f"An error occurred: {e}")
    else:
        print("No CSV file provided.")
