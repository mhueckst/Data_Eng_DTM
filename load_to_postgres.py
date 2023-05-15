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
    # parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()
    Datafile = args.datafile

    # CreateDB = args.createtable


def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    return connection


# create DF from csv, transform into sql ready dfs, load into postgres:
def copy_from_stringio(conn):
    csv = Datafile
    # df = pd.read_csv(csv)
    df = vt.transform_csv(csv)
    breadcrumb = vt.transform_BreadCrumb(df)
    trip = vt.transform_Trip(df)
    res = vt.read_csv_and_validate_all(csv)
    if not res:
        return 0
    trip, breadcrumb = res
    print("Dataframes created")

    # Load trip table using copy_from and stringIO buffer:
    buffer = io.StringIO()
    trip.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    start = time.perf_counter()

    cursor.copy_from(buffer, "trip", sep=",")

    elapsed = time.perf_counter() - start
    print(f"Finished Loading. Elapsed Time: {elapsed:0.4} seconds")

    # Load breadcrumb table using copy_from and stringIO buffer:
    buffer = io.StringIO()
    breadcrumb.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    start = time.perf_counter()

    cursor.copy_from(buffer, "breadcrumb", sep=",")

    elapsed = time.perf_counter() - start
    print(f"Finished Loading. Elapsed Time: {elapsed:0.4} seconds")

    return len(breadcrumb) + len(trip)


# If called from main, simply use the cmd argument.
# If called from another script, have the cmd provide a filename as the argument.
def main(filename=None):
    global Datafile

    try:
        if filename:
            Datafile = filename
        else:
            initialize()

        conn = dbconnect()
        rows_added_to_db = copy_from_stringio(conn)
        if rows_added_to_db == 0:
            raise Exception
        conn.commit()
        # If this function was called from outside (automated, send slack notif)
        if filename:
            send_slack_notification(
                f"Today's data was loaded to the database. Rows added: {rows_added_to_db}."
            )
        else:
            print(
                f"Today's data was loaded to the database. Rows added: {rows_added_to_db}."
            )
    except Exception as e:
        # If this function was called from outside (automated, send slack notif)
        if filename:
            send_slack_notification("Could not upload today's data to the database.")
        else:
            print("Could not upload today's data to the database.")
            print(e)
            traceback.print_exc()


if __name__ == "__main__":
    main()
