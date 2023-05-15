import sys

LABEL_STRING = "EVENT_NO_TRIP,EVENT_NO_STOP,OPD_DATE,VEHICLE_ID,METERS,ACT_TIME,GPS_LONGITUDE,GPS_LATITUDE,GPS_SATELLITES,GPS_HDOP"
error = False
try:
    source_file = sys.argv[1]
    dest_file = sys.argv[2]
except:
    print("Usage: python3 txt_to_csv.py <source txt file name> <dest csv file name>")
    error = True

if(not error):
    with open(source_file, 'r') as reader:
        data = reader.read()

    data = data.replace('"EVENT_NO_TRIP":', '')
    data = data.replace('"EVENT_NO_STOP":', '')
    data = data.replace('"OPD_DATE":', '')
    data = data.replace('"VEHICLE_ID":', "")
    data = data.replace('"METERS":', '')
    data = data.replace('"ACT_TIME":', '')
    data = data.replace('"GPS_LONGITUDE":', '')
    data = data.replace('"GPS_LATITUDE":', '')
    data = data.replace('"GPS_SATELLITES": ', '')
    data = data.replace('"GPS_HDOP":', '')
    data = data.replace('}', '')
    data = data.replace('{', '')
    data = data.replace('"', '')
    data = data.replace(' ', '')
    data = LABEL_STRING + '\n' + data

    with open(dest_file, 'w') as writer:
        writer.write(data)