import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import sys
from utilities import get_date_str

def fetch_html(url):
    """
    Fetches and returns the HTML content from a specified URL.

    :param url: URL to fetch HTML content from.
    :return: BeautifulSoup object containing the HTML content.
    """
    try:
        html_content = urlopen(url)
        return BeautifulSoup(html_content, "lxml")
    except Exception as e:
        sys.exit(f"Error fetching data from URL: {url} - {e}")

def extract_trip_ids(headers):
    """
    Extracts trip IDs from header tags in the HTML content.

    :param headers: List of BeautifulSoup header objects.
    :return: List of extracted trip IDs.
    """
    trip_ids = []
    for header in headers:
        header_text = header.get_text()
        trip_id = header_text.replace("Stop Events for ", "").replace(" for today", "")
        trip_ids.append(int(trip_id))
    return trip_ids

def parse_table_data(tables):
    """
    Parses table data from HTML and returns a DataFrame.

    :param tables: List of BeautifulSoup table objects.
    :return: DataFrame containing parsed table data.
    """
    data = []
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        cells = rows[1].find_all("td")
        cell_text = [cell.get_text() for cell in cells]
        data.append(cell_text)
    return pd.DataFrame(data)

def prepare_dataframe(data_df, trip_ids, columns):
    """
    Prepares and formats the DataFrame for output.

    :param data_df: DataFrame with raw data.
    :param trip_ids: List of trip IDs corresponding to the data.
    :param columns: List of column names for the DataFrame.
    :return: Formatted DataFrame.
    """
    data_df.columns = columns
    data_df["trip_id"] = trip_ids
    return data_df

def transform_trip_df(url):
    """
    Transforms trip data from a given URL into a DataFrame.

    :param url: URL to scrape trip data from.
    :return: Transformed DataFrame.
    """
    soup = fetch_html(url)
    headers = soup.find_all("h2")
    tables = soup.find_all("table")
    trip_ids = extract_trip_ids(headers)
    data_df = parse_table_data(tables)

    columns = ["vehicle_number", "leave_time", "train", "route_number", 
               "direction", "service_key", "trip_number", "stop_time",
               "arrive_time", "dwell", "location_id", "door", "lift", 
               "ons", "offs", "estimated_load", "maximum_speed", 
               "train_mileage", "pattern_distance", "location_distance", 
               "x_coordinate", "y_coordinate", "data_source", 
               "schedule_status"]
    return prepare_dataframe(data_df, trip_ids, columns)

def main(filename):
    """
    Main function to execute the script.

    :param filename: Filename to save the transformed data to.
    """
    try:
        df = transform_trip_df("http://www.psudataeng.com:8000/getStopEvents")
        df.to_csv(filename)
        print(f"Data transformed and saved to {filename}")
    except Exception as e:
        sys.exit(f"Error in processing: {e}")

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else f"/home/dtm-project/consumed_data/trips_{get_date_str()}.csv"
    main(filename)
