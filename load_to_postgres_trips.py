import time
import psycopg2
import argparse
import io
import pandas as pd
import validate_transform as vt
from send_slack_msg import send_slack_notification
import traceback


DBname = "postgres"
DBuser = "postgres"
DBpwd = "dtm"
# TableName = 'BreadCrumb'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
# CreateDB = False  # indicates whether the DB table should be (re)-created

Datafile = ""
CreateDB = ""

def initialize():
    global Datafile
    global CreateDB

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    args = parser.parse_args()
    Datafile = args.datafile

    


def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    return connection

def validate_transform_trips(df):
    print(df)
    df = df.drop(["Unnamed: 0"], axis=1)
    df = df.dropna()
    df = df.drop_duplicates(subset="trip_id", keep="first")
    print(df)
    return df

# create DF from csv, transform into sql ready dfs, load into postgres:
def copy_from_stringio(conn):
    
    stop_event_csv = Datafile

    # Get dataframes as validated csvs: 
    df = pd.read_csv(stop_event_csv)
    print(df)
    stop_event = validate_transform_trips(df)

    # Load trip table using copy_from and stringIO buffer:
    buffer = io.StringIO()
    stop_event.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    start = time.perf_counter()

    cursor.copy_from(buffer, "stopevent", sep=",")

    elapsed = time.perf_counter() - start
    print(f"Finished Loading. Elapsed Time: {elapsed:0.4} seconds")

    return len(stop_event)


# If called from main, simply use the cmd argument.
# If called from another script, have the cmd provide a filename as the argument.
def main(file=None):
    global Datafile

    try:
        if file:
            Datafile = file
        else:
            initialize()

        conn = dbconnect()
        rows_added_to_db = copy_from_stringio(conn)
        if rows_added_to_db == 0:
            raise Exception
        conn.commit()
        # If this function was called from outside (automated, send slack notif)
        if file:
            send_slack_notification(
                f"Today's stop-event data was loaded to the database. Rows added: {rows_added_to_db}."
            )
        else:
            print(
                f"Today's stop-event data was loaded to the database. Rows added: {rows_added_to_db}."
            )
        print(f"Rows added: {rows_added_to_db}")
    except Exception as e:
        # If this function was called from outside (automated, send slack notif)
        if file:
            send_slack_notification("Could not upload today's stop-event data to the database.")
        print("Could not upload today's stop-event data to the database.")
        print(e)
        traceback.print_exc()


if __name__ == "__main__":
    main()
