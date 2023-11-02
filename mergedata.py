import os
import pandas as pd
import random
import string
import argparse
import logging

def random_string(length=4):
    return ''.join(random.choices(string.ascii_letters, k=length))

def merge_csv_files(directory):
    if not os.path.isdir(directory):
        logging.error(f"The specified directory '{directory}' does not exist.")
        return

    output_identifier = random_string()
    merged_data = None

    for filename in os.listdir(directory):
        if "users" in filename and filename.endswith(".csv"):
            user_data = pd.read_csv(os.path.join(directory, filename))
            if merged_data is None:
                merged_data = user_data
            else:
                common_columns = list(set(merged_data.columns) & set(user_data.columns))
                if not common_columns:
                    logging.warning(f"No common columns found between '{filename}' and the merged data. Skipping.")
                    continue

                common_column = common_columns[0]  # Choose the first common column
                common_rows = min(1000, min(len(merged_data), len(user_data)))

                # Check if the chosen common column matches for at least 1000 rows
                match_count = (merged_data.head(common_rows)[common_column] == user_data.head(common_rows)[common_column]).sum()
                if match_count < common_rows:
                    logging.warning(f"Chosen common column '{common_column}' does not match for at least 1000 rows in '{filename}'. Skipping.")
                    continue

                # Merge dataframes
                merged_data = pd.merge(merged_data, user_data, on=common_column, how="inner")

    if merged_data is not None:
        # Delete duplicate columns
        merged_data = merged_data.loc[:, ~merged_data.columns.duplicated()]

        # Merge "firstname" and "lastname" columns if they exist
        if "firstname" in merged_data and "lastname" in merged_data:
            merged_data["fullname"] = merged_data["firstname"] + " " + merged_data["lastname"]
            merged_data = merged_data.drop(columns=["firstname", "lastname"])
        elif "first_name" in merged_data and "last_name" in merged_data:
            merged_data["fullname"] = merged_data["first_name"] + " " + merged_data["last_name"]
            merged_data = merged_data.drop(columns=["first_name", "last_name"])

        # If both "usernicename" and "username" exist, delete "usernicename" column
        if "usernicename" in merged_data and "username" in merged_data:
            merged_data = merged_data.drop(columns=["usernicename"])

        output_filename = f"merged_{output_identifier}.csv"
        output_path = os.path.join(directory, output_filename)

        merged_data.to_csv(output_path, index=False)
        logging.info(f"Merged data saved to {output_path}")
    else:
        logging.warning("No valid data found in the specified files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge CSV files in a directory.")
    parser.add_argument("directory_path", help="Path to the directory containing CSV files.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    merge_csv_files(args.directory_path)
