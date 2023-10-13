import pandas as pd
import os
import sys
import timeit
import random
import string

def merge_csv_files_in_directory(directory):
    # Create an empty dictionary to track dataframes by their filenames.
    dataframes = {}

    allowed_pii_columns = ['email', 'phone', 'ssn', 'id','user_id', 'customer_id', 'userid', 'customer_email',
                           'username',
                           'phonenumber', 'phone_number', 'billing_phone', 'billing_phone_number',
                           'billing_phone_number', 'billing_email', 'billing_email_address', 'billing_emailaddress',
                           'shipping_phone', 'shipping_phone_number', 'shipping_email', 'shipping_email_address',
                           'address', 'billing_address', 'shipping_address', 'address1', 'address2', 'address3']

    # Iterate through all files in the directory.
    for filename in os.listdir(directory):
        if filename.endswith(".csv") and "merged" not in filename:
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path, encoding='utf-8')

            common_columns = set(df.columns).intersection(allowed_pii_columns)

            if not common_columns:
                print(f"No common columns with allowed PII in {filename}. Skipping.")
                continue

            # Use the common column with the most matching values as the key column for merging.
            key_column = None
            max_matching_values = 0
            max_rows_to_check = 1000

            for column in common_columns:
                matching_values = 0

                # Limit the range to the first 1000 rows for both dataframes
                for i in range(min(len(df), max_rows_to_check)):
                    for j in range(min(len(df), max_rows_to_check)):
                        if df[column][i] == df[column][j]:
                            matching_values += 1

                if matching_values > max_matching_values:
                    max_matching_values = matching_values
                    key_column = column

            print(f"Using {key_column} as the key column for {filename}.")

            # Store the dataframe in the dictionary for later merging.
            dataframes[filename] = (df, key_column)

    # Check if there are dataframes that can be merged.
    if len(dataframes) < 2:
        print("Not enough dataframes to merge.")
        return

    # Merge dataframes one by one.
    merged_df = None
    for filename, (df, key_column) in dataframes.items():
        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=key_column)

    if merged_df is not None and merged_df.duplicated(subset=key_column).any():
        merged_df.drop_duplicates(subset=key_column, inplace=True)

    if 'firstname' in merged_df.columns and 'lastname' in merged_df.columns:
        merged_df['fullname'] = merged_df['firstname'] + " " + merged_df['lastname']
        merged_df.drop(columns=['firstname', 'lastname'], inplace=True)
    elif 'first_name' in merged_df.columns and 'last_name' in merged_df.columns:
        merged_df['fullname'] = merged_df['first_name'] + " " + merged_df['last_name']
        merged_df.drop(columns=['first_name', 'last_name'], inplace=True)

    if 'username' in merged_df.columns:
        merged_df.drop(columns=['usernicename'], errors='ignore', inplace=True)

    # Remove duplicate columns with the same row contents.
    merged_df = merged_df.T.drop_duplicates().T

    random_suffix = ''.join(random.choice(string.ascii_letters) for _ in range(4))
    output_filename = f"merged_{random_suffix}_{key_column}.csv"
    output_dir = os.path.join(directory, output_filename)
    merged_df.to_csv(output_dir, index=False)

    if os.path.exists(output_dir):
        print(f"Merged dataframes successfully and saved as '{output_filename}'.")
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
