import os
import pandas as pd
import random
import string
import argparse
import logging
import timeit
from usermetacleaner import usermeta_cleaner


def random_string(length=4):
    return ''.join(random.choices(string.ascii_letters, k=length))


def find_cleaned_usermeta_file(directory, usermeta_filename):
    cleaned_usermeta_filename = usermeta_filename.replace(".csv", "_usermeta_cleaned.csv")

    # Check if the cleaned usermeta file is in the same directory
    cleaned_usermeta_path = os.path.join(directory, cleaned_usermeta_filename)
    if os.path.isfile(cleaned_usermeta_path):
        return cleaned_usermeta_path
    else:
        return None


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

            # Check if the usermeta file is not cleaned
            if "umeta_id" in usermeta_csv and "meta_key" in usermeta_csv and "meta_value" in usermeta_csv:
                # Call the cleaning script
                logging.info("Cleaning the usermeta file...")
                usermeta_cleaner(os.path.join(directory, filename))
                # Look for the cleaned usermeta file
                cleaned_usermeta_path = find_cleaned_usermeta_file(directory, filename)
                if cleaned_usermeta_path:
                    usermeta_csv = pd.read_csv(cleaned_usermeta_path)

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
    try:
        users_data = pd.concat(users_data, ignore_index=True)
    except ValueError as e:
        if str(e) == "No objects to concatenate":
            logging.warning("No users data found.")
            users_data = pd.DataFrame()

    try:
        usermeta_data = pd.concat(usermeta_data, ignore_index=True)
    except ValueError as e:
        if str(e) == "No objects to concatenate":
            logging.warning("No usermeta data found.")
            usermeta_data = pd.DataFrame()

    min_matching_rows = 1000

    # Check matches for at least 1000 rows for big files
    if len(users_data) >= min_matching_rows and len(usermeta_data) >= min_matching_rows:
        # Check if both "id" and "userid" are present in users_data columns
        if "id" in users_data.columns and "userid" in users_data.columns:
            common_ids = set(users_data["id"]).intersection(usermeta_data["userid"])
        elif "id" in users_data.columns:
            common_ids = set(users_data["id"]) & set(usermeta_data["userid"])
        elif "userid" in users_data.columns:
            common_ids = set(users_data["userid"]) & set(usermeta_data["userid"])
        else:
            logging.warning("No common columns ('id' or 'userid') found in users_data.")
            return

    # Perform the merge with the filtered common IDs
    merged_data = users_data.merge(usermeta_data, how="inner", left_on="id" if "id" in users_data.columns else "userid", right_on="userid")


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

    # Cleaning up
    if "usernicename" in merged_data and "username" in merged_data:
        merged_data.drop("usernicename", axis=1, inplace=True)

    merged_data.drop_duplicates(inplace=True)

    output_filename = f"users_and_usermeta_{output_identifier}_merged.csv"
    output_path = os.path.join(directory, output_filename)

    merged_data.to_csv(output_path, index=False)
    logging.info(f"Merged data saved to {output_filename}")

    return output_path

def merge_with_sessions(directory):
    merged_users_usermeta_file = merge_csv_files(directory)

    sessions_file = None
    for filename in os.listdir(directory):
        if "sessions" in filename and "woocommerce" not in filename and filename.endswith(".csv"):
            sessions_file = os.path.join(directory, filename)
            break

    if sessions_file is None:
        logging.warning("No sessions file found in the specified directory.")
        return

    merged_data = pd.read_csv(merged_users_usermeta_file) if merged_users_usermeta_file else pd.DataFrame()
    sessions_data = pd.read_csv(sessions_file)

    # Check if potential merge columns exist
    potential_merge_columns = ['userid', 'email']

    for merge_column in potential_merge_columns:
        if merge_column in merged_data and merge_column in sessions_data:
            common_ids = set(merged_data[merge_column]).intersection(sessions_data[merge_column])

            if len(common_ids) >= 1000:
                merged_data = merged_data.merge(sessions_data, how="inner", on=merge_column)
                break

    if len(merged_data) == 0:
        logging.warning("No matching rows found for merging with sessions data.")
        return

    # Save the merged data with sessions
    output_filename = f"merged_with_sessions_{random_string()}.csv"
    output_path = os.path.join(directory, output_filename)

    merged_data.to_csv(output_path, index=False)
    logging.info(f"Merged data with sessions saved to {output_filename}")


if __name__ == "__main__":
    start = timeit.default_timer()

    parser = argparse.ArgumentParser(
        description="Merge CSV files in a directory and then merge with sessions data.")
    parser.add_argument(
        "directory_path", help="Path to the directory containing CSV files.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    merge_with_sessions(args.directory_path)

    stop = timeit.default_timer()
    print('Time taken: ', stop - start)