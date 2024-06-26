import pandas as pd
import os
import sys
import timeit


def find_mergeable_pairs(dataframes):
    mergeable_pairs = []
    merged_filenames = set()

    for filename1, (df1, key_column1) in dataframes.items():
        if filename1 in merged_filenames:
            continue  # Skip files that have already been merged

        for filename2, (df2, key_column2) in dataframes.items():
            if filename1 == filename2:
                continue  # Skip self-comparisons

            # Check if the dataframes have a common key column
            if key_column1 == key_column2 and key_column1 is not None:
                mergeable_pairs.append((filename1, filename2, key_column1))
                merged_filenames.add(filename1)
                merged_filenames.add(filename2)

    return mergeable_pairs


def merge_csv_files_in_directory(directory):
    # Create an empty dictionary to track dataframes by their filenames.
    dataframes = {}

    # List of possible PII.
    allowed_pii_columns = ['email', 'phone', 'ssn', 'user_id', 'customer_id', 'userid', 'customer_email', 'username',
                           'phonenumber', 'phone_number', 'billing_phone', 'billing_phone_number',
                           'billing_phone_number', 'billing_email', 'billing_email_address', 'billing_emailaddress',
                           'shipping_phone', 'shipping_phone_number', 'shipping_email', 'shipping_email_address',
                           'address', 'billing_address', 'shipping_address', 'address1', 'address2', 'address3']

    # Iterate through all files in the directory.
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path, encoding='utf-8')

            # Identify common columns with allowed PII columns.
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

    # Find mergeable pairs based on common key columns
    mergeable_pairs = find_mergeable_pairs(dataframes)
    merged_filenames = set()

    # Merge the compatible pairs
    for filename1, filename2, key_column in mergeable_pairs:
        df1, key_column1 = dataframes[filename1]
        df2, key_column2 = dataframes[filename2]

        # Ensure the key column exists in both dataframes
        if key_column in df1.columns and key_column in df2.columns:
            merged_df = pd.merge(df1, df2, on=key_column)
            merged_filenames.add(filename1)
            merged_filenames.add(filename2)

            if merged_df is not None and merged_df.duplicated(subset=key_column).any():
                merged_df.drop_duplicates(subset=key_column, inplace=True)

            # Update dataframes with the merged result
            dataframes[filename1] = (merged_df, key_column)
            del dataframes[filename2]

    # Combine first and last name
    for filename, (df, key_column) in dataframes.items():
        if ('firstname' in df.columns or 'first_name' in df.columns) and ('lastname' in df.columns
                                                                          or 'last_name' in df.columns):
            if 'first_name' in df.columns:
                df.rename(columns={'first_name': 'firstname'}, inplace=True)
            if 'last_name' in df.columns:
                df.rename(columns={'last_name': 'lastname'}, inplace=True)

            df['fullname'] = df['firstname'] + " " + df['lastname']
            df.drop(columns=['firstname', 'lastname'], inplace=True)

        # Drop usernicename if username exists in the dataframe
        if 'username' in df.columns:
            df.drop(columns=['usernicename'], errors='ignore', inplace=True)

    # Drop columns with the same row contents
    for filename, (df, key_column) in dataframes.items():
        dataframes[filename] = df.T.drop_duplicates().T

    # Save the merged dataframes to CSV files
    output_dir = os.path.join(directory, "merged")
    os.makedirs(output_dir, exist_ok=True)

    for filename, (df, key_column) in dataframes.items():
        output_path = os.path.join(output_dir, filename)
        df.to_csv(output_path, index=False)

        if os.path.exists(output_path):
            print(f"Merged dataframes for {filename} successfully and saved as '{output_path}'.")


if __name__ == "__main":
    # Start the timer.
    start = timeit.default_timer()

    # Check if the user provided a directory as a command line argument.
    if len(sys.argv) != 2:
        print("Please provide a directory containing CSV files as a command line argument.")
        sys.exit()

    directory = sys.argv[1]

    if not os.path.isdir(directory):
        print(f"'{directory}' is not a valid directory.")
        sys.exit()

    merge_csv_files_in_directory(directory)

    # Stop the timer.
    stop = timeit.default_timer()
    print('Time: ', stop - start)
