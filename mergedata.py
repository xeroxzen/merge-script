import os
import pandas as pd
import random
import string
import argparse
import logging
import timeit


def random_string(length=4):
    return ''.join(random.choices(string.ascii_letters, k=length))


def merge_csv_files(directory):
    if not os.path.isdir(directory):
        logging.error(f"The specified directory '{directory}' does not exist.")
        return

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

    # Check if the chosen columns exist in the DataFrames
    if "id" not in users_data[0] or "userid" not in usermeta_data[0]:
        logging.error(
            "The specified 'id' or 'userid' columns do not exist in the data.")
        return

    # Custom merge with matching rows check
    users_data = pd.concat(users_data, ignore_index=True)
    usermeta_data = pd.concat(usermeta_data, ignore_index=True)

    min_matching_rows = 1000

    # Check matches for at least 1000 rows for big files
    if len(users_data) >= min_matching_rows and len(usermeta_data) >= min_matching_rows:
        common_ids = set(users_data["id"]).intersection(
            usermeta_data["userid"])
    else:
        common_ids = set(users_data["id"]) & set(usermeta_data["userid"])

    if not common_ids:
        logging.warning("No matching rows found in 'id' and 'userid' columns.")
        return

    # Perform the merge with the filtered common IDs
    merged_data = users_data.merge(
        usermeta_data, how="inner", left_on="id", right_on="userid")

    # Delete duplicate columns
    merged_data.drop("userid", axis=1, inplace=True)

    # Merge 'firstname' and 'lastname' columns
    if "firstname" in merged_data and "lastname" in merged_data:
        merged_data['fullname'] = merged_data['firstname'] + \
            ' ' + merged_data['lastname']
        merged_data.drop(["firstname", "lastname"], axis=1, inplace=True)
    elif "first_name" in merged_data and "last_name" in merged_data:
        merged_data['fullname'] = merged_data['first_name'] + \
            ' ' + merged_data['last_name']
        merged_data.drop(["first_name", "last_name"], axis=1, inplace=True)

    # If "usernicename" and "username" both exist, delete "usernicename" column
    if "usernicename" in merged_data and "username" in merged_data:
        merged_data.drop("usernicename", axis=1, inplace=True)

    # Delete duplicate rows
    merged_data.drop_duplicates(inplace=True)

    output_filename = f"merged_{output_identifier}.csv"
    output_path = os.path.join(directory, output_filename)

    merged_data.to_csv(output_path, index=False)
    logging.info(f"Merged data saved to {output_filename}")


if __name__ == "__main__":
    start = timeit.default_timer()

    parser = argparse.ArgumentParser(
        description="Merge CSV files in a directory.")
    parser.add_argument(
        "directory_path", help="Path to the directory containing CSV files.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    merge_csv_files(args.directory_path)

    stop = timeit.default_timer()
    print('Time taken: ', round((stop - start)/60, 1), "minutes")
