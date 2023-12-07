import pandas as pd
import sys
from utilities import get_date_str

def transform_trip_df(file_path):
    """
    Transforms trip data from a CSV file into a DataFrame.
    
    :param file_path: Path to the CSV file.
    :return: Transformed DataFrame.
    """
    # Read data from the CSV file
    data_df = pd.read_csv(file_path)

    # Remove any leading/trailing spaces in column names and data
    data_df.columns = data_df.columns.str.strip()
    data_df = data_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return data_df

if __name__ == "__main__":
    # Check if the file path is provided as a command-line argument
    if len(sys.argv) < 2:
        # Default file path if no argument is provided
        file_path = "/Users/mahshid/Downloads/test_data.csv"
    else:
        file_path = sys.argv[1]

    transformed_df = transform_trip_df(file_path)

    # Optionally, save the transformed data back to a file
    # You can change the output file path as needed.
    output_file_path = f"/Users/mahshid/Downloads/transformed_data_{get_date_str()}.csv"
    transformed_df.to_csv(output_file_path)
    print(f"Data transformed and saved to {output_file_path}")

