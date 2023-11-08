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
    cleaned_usermeta_filename = usermeta_filename.replace(
        ".csv", "_usermeta_cleaned.csv")

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

    users_data, usermeta_data, sessions_data = [], [], []

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
                cleaned_usermeta_path = find_cleaned_usermeta_file(
                    directory, filename)
                if cleaned_usermeta_path:
                    # Now, load the cleaned usermeta data
                    usermeta_csv = pd.read_csv(cleaned_usermeta_path)

            usermeta_data.append(usermeta_csv)
        elif "session" in filename and filename.endswith(".csv"):
            sessions_csv = pd.read_csv(os.path.join(
                directory, filename), dtype="object")
            sessions_data.append(sessions_csv)

    # Check if sessions_data exists
    if sessions_data:
        # Extract the first session data DataFrame
        sessions_data = sessions_data[0]
        key_columns = set(sessions_data.columns).intersection(
            users_data[0].columns)
        # Check if "userid" or "email" columns exist in users_data and sessions_data
        if "userid" in users_data[0].columns and "userid" in sessions_data.columns:
            key_columns = set(users_data[0]["userid"]) & set(
                sessions_data["userid"])
        elif "email" in users_data[0].columns and "email" in sessions_data.columns:
            key_columns = set(users_data[0]["email"]) & set(
                sessions_data["email"])
        else:
            logging.warning(
                "No common columns ('userid' or 'email') found for sessions merge")
            key_columns = set()

        if key_columns:
            # Filter sessions_data based on common key_column
            sessions_data = sessions_data[sessions_data[key_columns].isin(
                users_data[0][key_columns])]
            # Perform the merge with users_data
            merged_data = users_data[0].merge(
                sessions_data, how="inner", left_on=key_columns, right_on=key_columns)
        else:
            # If no key column, just merge the users_data with an empty sessions_data
            merged_data = users_data[0]
    else:
        # If no sessions data, just use users_data
        merged_data = users_data[0]

    if not users_data or not usermeta_data:
        logging.error("No valid data found in the specified files.")
        return

    # Check if the chosen columns exist in the DataFrames
    if ("id" not in users_data[0]) or "userid" not in usermeta_data[0]:
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

    try:
        sessions_data = pd.concat(sessions_data, ignore_index=True)
    except ValueError as e:
        if str(e) == "No objects to concatenate":
            logging.warning("No sessions data found.")
            sessions_data = pd.DataFrame()

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

    # Cleaning up
    if "usernicename" in merged_data and "username" in merged_data:
        merged_data.drop("usernicename", axis=1, inplace=True)

    merged_data.drop_duplicates(inplace=True)

    output_filename = f"users_and_usermeta_{output_identifier}_merged.csv"
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
    print('Time taken: ', stop - start)
