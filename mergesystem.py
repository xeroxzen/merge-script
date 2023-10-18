import pandas as pd
import os
import sys
import timeit
import random
import string

# Define the supported web systems and their common PII columns
WEB_SYSTEMS = {
    "wordpress": ["email", "username", "fullname", "userid", "firstname", "lastname", "usernicename",
                  "hashed_password", "first_name", "last_name", "user_email", "user_login", "user_pass", "phone",
                  "email"],
    "joomla": ["email", "username", "usernicename", "name", "userid", "first_name", "last_name", "password",
               "nickname", "comment_author_email", "comment_author", "comment_author_url", "comment_author_ip"],
    "drupal": ["email", "name", "phone"],
    "magento": ["customer_email", "customer_name", "billing_name", "billing_email", "billing_phone", "shipping_name",
                "shipping_address", "shipping_phone", "shipping_email"],
    "shopify": ["email", "phone", "name", "first_name", "last_name", "billing_address", "shipping_address"]
}

def get_system_key_columns(df):
    for system, common_columns in WEB_SYSTEMS.items():
        common_columns_in_df = set(common_columns).intersection(df.columns)
        if common_columns_in_df:
            return system, common_columns_in_df.pop()
    return None, None

def check_matching_rows(df, key_column, min_matching_records=1000):
    matching_rows = df.duplicated(subset=[key_column])
    return matching_rows.sum() >= min_matching_records

def merge_csv_files_in_directory(directory):
    # Create an empty dictionary to track dataframes by their filenames.
    dataframes = {}

    # Iterate through all files in the directory.
    for filename in os.listdir(directory):
        if filename.endswith(".csv") and "merged" not in filename:
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path, encoding='utf-8')

            system, key_column = get_system_key_columns(df)

            if not system:
                print(f"No recognized system in {filename}. Skipping.")
                continue

            print(f"Recognized system: {system}, using {key_column} as the key column for {filename}.")

            # Check for matching rows in the key column
            if not check_matching_rows(df, key_column):
                print(f"Not enough matching rows in {filename}. Skipping.")
                continue

            # Store the dataframe in the dictionary for later merging.
            dataframes.setdefault(system, []).append((df, key_column))

    # Check if there are dataframes that can be merged for each recognized system.
    for system, dfs in dataframes.items():
        if len(dfs) < 2:
            print(f"Not enough dataframes to merge for {system}.")
            continue

        # Merge dataframes for the same recognized system.
        merged_df = None
        for df, key_column in dfs:
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on=key_column)

        if merged_df is not None and merged_df.duplicated(subset=key_column).any():
            merged_df.drop_duplicates(subset=key_column, inplace=True)

        # Additional merging and data cleaning logic can be added here for each system.

        # Save the merged dataframes for each system.
        random_suffix = ''.join(random.choice(string.ascii_letters) for _ in range(4))
        output_filename = f"merged_{random_suffix}_{key_column}_{system}.csv"
        output_dir = os.path.join(directory, output_filename)
        merged_df.to_csv(output_dir, index=False)

        if os.path.exists(output_dir):
            print(f"Merged dataframes for the {system} system successfully and saved as '{output_filename}'.")
            print(merged_df.head())

        return merged_df


if __name__ == "__main__":
    start = timeit.default_timer()

    if len(sys.argv) != 2:
        print("Please provide a directory containing CSV files as a command line argument.")
        sys.exit()

    directory = sys.argv[1]

    if not os.path.isdir(directory):
        print(f"'{directory}' is not a valid directory.")
        sys.exit()

    merge_csv_files_in_directory(directory)

    stop = timeit.default_timer()
    print('Time: ', stop - start)
