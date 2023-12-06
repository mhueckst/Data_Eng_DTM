import pytz
from datetime import datetime

#Converts current UTC (Coordinated Universal Time) time zone time to US/Pacific timezone time
def get_date_str():
    return datetime.now(tz=pytz.utc).astimezone(pytz.timezone("US/Pacific")).strftime("%Y-%m-%d")
