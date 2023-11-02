import os
import pandas as pd
import random
import string
import argparse
import logging

def random_string(length=4):
    return ''.join(random.choices(string.ascii_letters, k=length))

def merge_csv_files(directory):
    # Check if the directory exists
    if not os.path.isdir(directory):
        logging.error(f"The specified directory '{directory}' does not exist.")
        return

    users_merge_column = "id"
    usermeta_merge_column = "userid"
    output_identifier = random_string()

    users_data, usermeta_data = [], []

    for filename in os.listdir(directory):
        if "users" in filename and filename.endswith(".csv"):
            users_csv = pd.read_csv(os.path.join(directory, filename))
            users_data.append(users_csv)
        elif "usermeta" in filename and filename.endswith(".csv"):
            usermeta_csv = pd.read_csv(os.path.join(directory, filename))
            usermeta_data.append(usermeta_csv)

    if not users_data or not usermeta_data:
        logging.error("No valid data found in the specified files.")
        return

    merged_data = pd.concat(users_data, ignore_index=True)
    merged_data = pd.merge(merged_data, pd.concat(usermeta_data, ignore_index=True),
                           left_on=users_merge_column, right_on=usermeta_merge_column, how="inner")

    output_filename = f"merged_{output_identifier}.csv"
    output_path = os.path.join(directory, output_filename)

    merged_data.to_csv(output_path, index=False)
    logging.info(f"Merged data saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge CSV files in a directory.")
    parser.add_argument("directory_path", help="Path to the directory containing CSV files.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    merge_csv_files(args.directory_path)
