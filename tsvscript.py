#!/usr/bin/python3
import csv
import json
import sys
from geojson import Feature, FeatureCollection, Point

def create_sample_tsv(file_path):
    """
    Creates a sample TSV file for testing purposes.
    
    :param file_path: Path to the TSV file to be created.
    """
    sample_data = [
        ["-122.4194", "37.7749", "60"],
        ["-122.4184", "37.7759", "65"],
        ["-122.4174", "37.7769", "70"]
    ]

    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerows(sample_data)

def read_tsv_data(file_path):
    """
    Reads TSV (Tab-Separated Values) data from a file.
    
    :param file_path: Path to the TSV file.
    :return: List of data rows.
    """
    with open(file_path, newline='') as csvfile:
        return list(csv.reader(csvfile, delimiter='\t'))

def create_geojson_features(data):
    """
    Converts TSV data into GeoJSON features.

    :param data: List of data rows from TSV file.
    :return: List of GeoJSON features.
    """
    features = []
    for row in data:
        try:
            longitude, latitude, speed = map(float, row)
            features.append(
                Feature(
                    geometry=Point((longitude, latitude)),
                    properties={'speed': speed}
                )
            )
        except ValueError:
            continue
    return features

def write_geojson_file(features, output_file):
    """
    Writes GeoJSON features to a file.

    :param features: List of GeoJSON features.
    :param output_file: Path to the output file.
    """
    collection = FeatureCollection(features)
    with open(output_file, "w") as f:
        json.dump(collection, f)

if __name__ == "__main__":
    input_file = 'sample_data.tsv'
    output_file = 'output.geojson'

    create_sample_tsv(input_file)
    tsv_data = read_tsv_data(input_file)
    geojson_features = create_geojson_features(tsv_data)
    write_geojson_file(geojson_features, output_file)

    print(f"GeoJSON data written to {output_file}")



