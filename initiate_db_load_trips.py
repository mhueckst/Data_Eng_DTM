import load_to_postgres
from utilities import get_date_str

# Get today's date
filename = f"/home/dtm-project/consumed_data/trips_{get_date_str()}.csv"
load_to_postgres_trips.main(filename)
