import pytz
from datetime import datetime

def get_date_str():
    return datetime.now(tz=pytz.utc).astimezone(pytz.timezone("US/Pacific")).strftime("%Y-%m-%d")
