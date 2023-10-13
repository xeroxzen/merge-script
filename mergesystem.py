import pandas as pd
import os
import sys
import timeit
import random
import string

# Define the supported web systems and their common PII columns
WEB_SYSTEMS = {
    "wordpress": ["email", "username", "fullname"],
    "joomla": ["email", "username", "fullname"],
    "drupal": ["email", "username", "fullname"],
    "magento": ["email", "username"],
    "moodle": ["email", "username", "fullname"],
    # Add more systems as needed
}


def get_system_key_columns(df):
    for system, common_columns in WEB_SYSTEMS.items():
        common_columns_in_df = set(common_columns).intersection(df.columns)
        if common_columns_in_df:
            return system, common_columns_in_df.pop()
    return None, None


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
