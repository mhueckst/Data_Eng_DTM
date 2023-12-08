# Importing necessary modules
import load_to_postgres  # Import the module responsible for loading data into PostgreSQL
from utilities import get_date_str  # Import 'get_date_str' function from utilities to get today's date in string format

# The purpose of this script is to initiate the process of loading data into a PostgreSQL database.

# Constructing the filename for the data file to be loaded
# The filename includes today's date, ensuring that the data corresponds to the current day
# 'get_date_str()' is used to append a formatted date string to the filename
filename = f"/home/consumed_data/{get_date_str()}.csv"

# Execute the main function from the 'load_to_postgres' module
# This function is tasked with loading the data from the specified CSV file into the PostgreSQL database
load_to_postgres.main(filename)

