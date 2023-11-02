import os
import pandas as pd
import random
import string
import sys

def random_string(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def merge_csv_files(directory):
    files = os.listdir(directory)

    # Initialize empty DataFrames to store merged data
    users_data = pd.DataFrame()
    usermeta_data = pd.DataFrame()

    # Iterate through the files and merge them based on filenames
    for filename in files:
        if "users" in filename and filename.endswith(".csv"):
            users_csv = pd.read_csv(os.path.join(directory, filename))
            users_data = pd.concat([users_data, users_csv], ignore_index=True)
        elif "usermeta" in filename and filename.endswith(".csv"):
            usermeta_csv = pd.read_csv(os.path.join(directory, filename))
            usermeta_data = pd.concat(
                [usermeta_data, usermeta_csv], ignore_index=True)

    # Define common columns to merge on
    users_merge_column = "id"
    usermeta_merge_column = "userid"

    # Merge dataframes if they are not empty
    if not users_data.empty and not usermeta_data.empty:
        merged_data = pd.merge(users_data, usermeta_data, left_on=users_merge_column,
                               right_on=usermeta_merge_column, how="inner")

        # Generate a random string for the output filename
        random_str = random_string()
        output_filename = f"merged_{random_str}.csv"
        output_dir = os.path.join(directory, output_filename)

        # Write the merged data to a new CSV file
        merged_data.to_csv(output_dir, index=False)

        print(f"Merged data saved to {output_filename}")
    else:
        print("No data to merge in the specified files.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python merge_csv.py <directory_path>")
    else:
        directory_path = sys.argv[1]
        merge_csv_files(directory_path)
