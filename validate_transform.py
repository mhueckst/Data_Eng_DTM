import pandas as pd
from datetime import datetime, timedelta
import sys, math
from send_slack_msg import send_slack_notification

# Get csv from command line. If not provided, ask for csv by input


# *************
# Read csv and transform it:
def transform_csv(csv):
    df = pd.read_csv(csv)
    df = df.drop(columns=["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"])

    def create_timestamp(i):
        date = datetime.strptime(i["OPD_DATE"], "%d%b%Y:%H:%M:%S")
        time = timedelta(seconds=i["ACT_TIME"])
        return date + time

    df["TIMESTAMP"] = df.apply(create_timestamp, axis=1)
    # df = df.drop(columns=["OPD_DATE", "ACT_TIME"])

    def add_speed_column(df):
        # Calculate speeds using pd diff
        def get_speed(row):
            return row["dMETERS"] / row["dTIMESTAMP"].total_seconds()

        df["dMETERS"] = df["METERS"].diff()
        df["dTIMESTAMP"] = df["TIMESTAMP"].diff()
        df["SPEED"] = df.apply(get_speed, axis=1)

        # Go through each record. Each record that has a new EVENT_NO_TRIP should take the speed from the next record (the second record)
        prev_no_trip = -1
        for i in range(len(df)):
            current_no_trip = df.iloc[i]["EVENT_NO_TRIP"]
            if current_no_trip != prev_no_trip:
                # Make sure a record exists after and belongs to the same trip
                if (
                    i + 1 < len(df)
                    and df.iloc[i + 1]["EVENT_NO_TRIP"] == current_no_trip
                    and not math.isnan(df.iloc[i + 1]["SPEED"])
                ):
                    df.at[i, "SPEED"] = df.iloc[i + 1]["SPEED"]
                # Otherwise, just put 0
                else:
                    df.at[i, "SPEED"] = 0
                prev_no_trip = current_no_trip

        return df.drop(columns=["dMETERS", "dTIMESTAMP"])

    df["dMETERS"] = df["METERS"].diff()
    df["dTIMESTAMP"] = df["TIMESTAMP"].diff()
    df = add_speed_column(df)
    # df['SPEED'] = df.apply(lambda row: speed(row['dMETERS'],row['dTIMESTAMP'].total_seconds()), axis=1)
    # df = df.drop(columns=['dMETERS','dTIMESTAMP'])

    # This is a hot fix for the NaN first speed value!
    # df.sort_values(by='EVENT_NO_TRIP', na_position='first')
    # df.iloc[0,6] = df.iloc[1,6]
    return df


# ****************
# Transform data into postgres table format:
def transform_BreadCrumb(df):
    BreadCrumb = df[
        ["TIMESTAMP", "GPS_LATITUDE", "GPS_LONGITUDE", "SPEED", "EVENT_NO_TRIP"]
    ].copy()
    BreadCrumb.rename(
        {
            "TIMESTAMP": "tstamp",
            "GPS_LATITUDE": "latitude",
            "GPS_LONGITUDE": "longitude",
            "SPEED": "speed",
            "EVENT_NO_TRIP": "trip_id",
        },
        axis="columns",
        inplace=True,
    )
    return BreadCrumb


def transform_Trip(df):
    Trip = df[["EVENT_NO_TRIP", "VEHICLE_ID"]].copy()
    Trip.rename(
        {"EVENT_NO_TRIP": "trip_id", "VEHICLE_ID": "vehicle_id"},
        axis="columns",
        inplace=True,
    )
    Trip.insert(loc=1, column="route_id", value=-1)
    Trip.insert(loc=3, column="service_key", value="Weekday")
    Trip.insert(loc=4, column="direction", value="Out")
    Trip = Trip.drop_duplicates(subset=["trip_id"])

    return Trip


# Remove rows where speed is greater than 45 m/s (~ 100mph)
# Return the number of rows removed
def speed_validation(df):
    original_length = len(df)
    df = df[df["speed"].between(0, 45)]
    # Return the number of rows dropped
    return df, original_length - len(df)


# Remove rows that have latitude way outside of
# Portland's latitude
def latitude_validation(df):
    original_length = len(df)
    df = df[df["latitude"].between(44, 47)]
    # Return the number of rows dropped
    return df, original_length - len(df)


# Remove rows that have longitude way outside of
# Portland's longitude
def longitude_validation(df):
    original_length = len(df)
    df = df[df["longitude"].between(-124, -121)]
    # Return the number of rows dropped
    return df, original_length - len(df)


# Meters of the same trip should not decrease
def increasing_meters_validation(df):
    trip_id = -1
    meters = 0
    violations = 0
    to_drop = []
    for i in range(len(df)):
        # If new trip_id, restart meters
        if df.iloc[i]["EVENT_NO_TRIP"] != trip_id:
            trip_id = df.iloc[i]["EVENT_NO_TRIP"]
            meters = 0
        elif df.iloc[i]["METERS"] < meters:
            violations += 1
            to_drop.append(i)
        meters = df.iloc[i]["METERS"]
    return df.drop(to_drop, axis=0), violations


# There should be more breadcrumbs during the day (8am to 8pm)
def day_vs_night_validation(df):
    day = len(df["ACT_TIME"].between(28800, 72000))
    night = len(df) - day
    if night > day:
        return False
    return True


# Make sure there are no empty values (drop those rows)
def exists_validation(df):
    original_length = len(df)
    df = df.dropna()
    return df, original_length - len(df)


# Every trip should have the same vehicle number
def same_vehicle_id_validation(df):
    to_drop = []
    prev_trip_id = -1
    prev_vehicle_id = -1
    for i in range(len(df)):
        # If different vehicle ID, make sure different trip ID as well
        if df.iloc[i]["VEHICLE_ID"] != prev_vehicle_id:
            if df.iloc[i]["EVENT_NO_TRIP"] == prev_trip_id:
                to_drop.append(i)
            else:
                prev_trip_id = df.iloc[i]["EVENT_NO_TRIP"]
                prev_vehicle_id = df.iloc[i]["VEHICLE_ID"]
    return df.drop(to_drop, axis=0), len(to_drop)


# Total number of records should not be more than 5 million
def record_count_validation(df):
    if len(df) > 5000000:
        return False
    return True


# Detect duplicates. Look for consecutive records that have the same vehicle and and time
def no_duplicate_validation(df):
    prev_vehicle_id = -1
    prev_time = ""
    to_drop = []
    for i in range(len(df)):
        if (
            df.iloc[i]["VEHICLE_ID"] == prev_vehicle_id
            and df.iloc[i]["TIMESTAMP"] == prev_time
        ):
            to_drop.append(i)
        prev_vehicle_id = df.iloc[i]["VEHICLE_ID"]
        prev_time = df.iloc[i]["TIMESTAMP"]
    return df.drop(to_drop, axis=0), len(to_drop)


# Make sure the ACT_TIME is within reasonable range (up to 172800)
def act_time_validation(df):
    original_length = len(df)
    df = df[df["ACT_TIME"].between(0, 172800)]
    return df, original_length - len(df)


# Validate all:
def read_csv_and_validate_all(csv):
    df = transform_csv(csv)
    breadcrumbs = transform_BreadCrumb(df)
    trips = transform_Trip(df)

    violations_str = ""

    # Validation of trip data
    df, exists_violations = exists_validation(df)
    violations_str += "Exists violations removed: " + str(exists_violations) + "\n"
    print("Exists violations removed: " + str(exists_violations))

    df, no_duplicate_violations = no_duplicate_validation(df)
    violations_str += (
        "No duplicates violations removed: " + str(no_duplicate_violations) + "\n"
    )
    print("No duplicates violations removed: " + str(no_duplicate_violations))

    record_count_res = record_count_validation(df)
    if not record_count_res:
        send_slack_notification("Fatal violation: Too many records (> 5 million)")
        print("Fatal violation: Too many records (> 5 million)")
        return None

    day_vs_night_res = day_vs_night_validation(df)
    if not day_vs_night_res:
        send_slack_notification("Fatal violation: More night records than day records")
        print("Fatal violation: More night records than day records")
        return None

    df, act_time_violations = act_time_validation(df)
    violations_str += "ACT_TIME violations removed: " + str(act_time_violations) + "\n"
    print("ACT_TIME violations removed: " + str(act_time_violations))

    df, same_vehicle_id_violations = same_vehicle_id_validation(df)
    violations_str += (
        "Same vehicle ID violations removed: " + str(same_vehicle_id_violations) + "\n"
    )
    print("Same vehicle ID violations removed: " + str(same_vehicle_id_violations))

    df, increasing_meters_violations = increasing_meters_validation(df)
    violations_str += (
        "Increasing meters violations removed: "
        + str(increasing_meters_violations)
        + "\n"
    )
    print("Increasing meters violations removed: " + str(increasing_meters_violations))

    # Validation of breadcrumb data
    breadcrumbs, speed_violations = speed_validation(breadcrumbs)
    violations_str += "Speed violations removed: " + str(speed_violations) + "\n"
    print("Speed violations removed: " + str(speed_violations))

    breadcrumbs, latitude_violations = latitude_validation(breadcrumbs)
    violations_str += "Latitude violations removed: " + str(latitude_violations) + "\n"
    print("Latitude violations removed: " + str(latitude_violations))

    breadcrumbs, longitude_violations = longitude_validation(breadcrumbs)
    violations_str += (
        "Longitude violations removed: " + str(longitude_violations) + "\n"
    )
    print("Longitude violations removed: " + str(longitude_violations))

    print(violations_str)

    return trips, breadcrumbs


if __name__ == "__main__":
    csv = ""
    try:
        csv = sys.argv[1]
    except:
        # If cmd arg not provided, ask by input
        csv = input("CSV file to transform: ")

    res = read_csv_and_validate_all(csv)
    trip, breadcrumb = res
    print(breadcrumb)
