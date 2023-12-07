# Importing required functions from other modules
from load_to_postgres_trips import load_to_postgres_trips  # Importing the main function from the 'load_to_postgres_trips' module
from utilities import get_date_str  # Importing 'get_date_str' function from the 'utilities' module to get a formatted date string

# The main goal of this script is to initiate the process of loading trip data into a PostgreSQL database.

# Constructing the file path for the trips data file
# This file path includes the date to ensure that the data corresponds to the current day
# The 'get_date_str()' function from 'utilities' module returns a string representation of today's date
filename = f"/home/dtm-project/consumed_data/trips_{get_date_str()}.csv"

# Calling the main function from the 'load_to_postgres_trips' module
# The function is responsible for loading the data from the CSV file (specified by 'filename') into the PostgreSQL database
load_to_postgres_trips.main(filename)

