import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import sys
from utilities import get_date_str

def transform_trip_df():
    URL = "http://www.psudataeng.com:8000/getStopEvents"
    html = urlopen(URL)
    soup = BeautifulSoup(html, "lxml")

    h2s = soup.find_all("h2")
    tables = soup.find_all("table")

    list_h2 = []
    for header in h2s:
        header_str = str(header).replace("<h2>Stop Events for ", "").replace(" for today</h2>", "")
        list_h2.append(int(header_str))

    labels = ["vehicle_number", "leave_time", "train", "route_number", "direction", "service_key", "trip_number", "stop_time","arrive_time","dwell","location_id","door","lift","ons","offs","estimated_load","maximum_speed","train_mileage","pattern_distance","location_distance","x_coordinate","y_coordinate","data_source","schedule_status"]
    labels_df = pd.DataFrame(labels).T

    data = []
    for i in range(len(tables)):
        table = tables[i]

        # First row is the label row. Get second row for the table's data.
        # Is no second row, there's no row with data. Just leave
        this_table = table.find_all("tr")
        if(len(this_table) < 2):
            continue
        this_table = this_table[1]
        cells = this_table.find_all("td")
        str_cells = str(cells)
        clean = re.compile("<.*?>")
        clean2 = re.sub(clean, "", str_cells)
        clean3 = clean2.replace("[", "").replace("]", "")
        data.append(clean3)

    data_df = pd.DataFrame(data)
    data_df = data_df[0].str.split(",", expand=True)
    # print(data_df)

    labels = ["vehicle_number", "leave_time", "train", "route_number", "direction", "service_key", "trip_number", "stop_time","arrive_time","dwell","location_id","door","lift","ons","offs","estimated_load","maximum_speed","train_mileage","pattern_distance","location_distance","x_coordinate","y_coordinate","data_source","schedule_status"]
    labels_df = pd.DataFrame(labels).T
    columns_to_keep = ["trip_id", "route_number", "service_key", "direction"]

    new_df = pd.concat([labels_df, data_df])
    new_df = new_df.rename(columns=new_df.iloc[0])
    new_df = new_df.drop(new_df.index[0])

    new_df["trip_id"] = pd.DataFrame(list_h2)
    new_df = new_df[columns_to_keep]

    return new_df

if __name__ == "__main__":
    if(len(sys.argv) < 2):
        filename = f"/home/dtm-project/consumed_data/trips_{get_date_str()}.csv"
        print(filename)
    else:
        filename = sys.argv[1]
    transform_trip_df(filename).to_csv(filename)


