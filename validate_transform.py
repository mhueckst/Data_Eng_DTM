import pandas as pd
from datetime import datetime, timedelta
import sys, math
from send_slack_msg import send_slack_notification

def read_csv_file(file_path):
    """
    Reads a CSV file into a pandas DataFrame.

    :param file_path: Path to the CSV file.
    :return: DataFrame containing the CSV data.
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        send_slack_notification(f"Error reading CSV file: {e}")
        sys.exit(f"Error reading CSV file: {e}")

def create_timestamp(row):
    """
    Creates a timestamp from OPD_DATE and ACT_TIME columns.

    :param row: DataFrame row containing OPD_DATE and ACT_TIME.
    :return: Combined datetime object.
    """
    date = datetime.strptime(row["OPD_DATE"], "%d%b%Y:%H:%M:%S")
    time = timedelta(seconds=row["ACT_TIME"])
    return date + time

def calculate_speed(row):
    """
    Calculates the speed from distance and time differences.

    :param row: DataFrame row containing dMETERS and dTIMESTAMP.
    :return: Calculated speed.
    """
    if row['dTIMESTAMP'].total_seconds() > 0:
        return row['dMETERS'] / row['dTIMESTAMP'].total_seconds()
    return 0

def transform_csv(df):
    """
    Performs data transformations on the DataFrame.

    :param df: DataFrame to be transformed.
    :return: Transformed DataFrame.
    """
    df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)
    df['dMETERS'] = df['METERS'].diff()
    df['dTIMESTAMP'] = df['TIMESTAMP'].diff()
    df['SPEED'] = df.apply(calculate_speed, axis=1)
    return df.drop(columns=['dMETERS', 'dTIMESTAMP'])

def rename_columns(df, column_mappings):
    """
    Renames columns of the DataFrame based on a mapping dictionary.

    :param df: DataFrame whose columns are to be renamed.
    :param column_mappings: Dictionary mapping old column names to new ones.
    :return: DataFrame with renamed columns.
    """
    return df.rename(column_mappings, axis='columns')

def validate_data(df, column, min_value, max_value):
    """
    Validates data in a DataFrame column based on a min and max range.

    :param df: DataFrame to be validated.
    :param column: Column name to be validated.
    :param min_value: Minimum acceptable value.
    :param max_value: Maximum acceptable value.
    :return: DataFrame after validation.
    """
    return df[df[column].between(min_value, max_value)]

def main():
    """
    Main function to execute the script.
    """
    if len(sys.argv) < 2:
        print("Usage: python script.py <csv_file_path>")
        sys.exit(1)

    csv_path = sys.argv[1]
    df = read_csv_file(csv_path)

    # Data transformation
    df = transform_csv(df)

    # Renaming columns for BreadCrumb
    breadcrumb_columns = {
        'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude',
        'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed', 'EVENT_NO_TRIP': 'trip_id'
    }
    breadcrumbs = rename_columns(df, breadcrumb_columns)

    # Renaming columns for Trip
    trip_columns = {
        'EVENT_NO_TRIP': 'trip_id', 'VEHICLE_ID': 'vehicle_id',
        'route_id': -1, 'service_key': 'Weekday', 'direction': 'Out'
    }
    trips = rename_columns(df, trip_columns).drop_duplicates(subset=['trip_id'])

    # Data validation
    breadcrumbs = validate_data(breadcrumbs, 'speed', 0, 45)
    breadcrumbs = validate_data(breadcrumbs, 'latitude', 44, 47)
    breadcrumbs = validate_data(breadcrumbs, 'longitude', -124, -121)

    # Further processing...
    print(breadcrumbs.head())
    print(trips.head())

if __name__ == "__main__":
    main()

