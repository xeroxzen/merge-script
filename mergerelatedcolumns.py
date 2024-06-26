import pandas as pd
import sys
import os
import timeit

def merge_csv_files(file1, file2):
    """
    Merges two CSV files that have related columns in common and have row values that match.

    Args:
        file1: The first CSV file.
        file2: The second CSV file.

    Returns:
        A merged CSV file, or None if the dataframes cannot be merged.
    """

    # Read the CSV files into Pandas dataframes.
    df1 = pd.read_csv(file1, encoding='utf-8')
    df2 = pd.read_csv(file2, encoding='utf-8')

    # List of possible PII.
    allowed_pii_columns = ['email', 'phone', 'ssn', 'user_id', 'customer_id', 'userid', 'customer_email', 'username',
                           'phonenumber','phone_number', 'billing_phone', 'billing_phone_number',
                           'billing_phone_number', 'billing_email', 'billing_email_address', 'billing_emailaddress',
                           'shipping_phone', 'shipping_phone_number', 'shipping_email', 'shipping_email_address',
                           'address','billing_address', 'shipping_address', 'address1', 'address2', 'address3']

    # List of the common columns between the two dataframes.
    common_columns = set(df1.columns).intersection(df2.columns).intersection(allowed_pii_columns)

    # If there are no common columns, return None.
    if not common_columns:
        print("The dataframes do not have any common columns")
        return None

    # If there is only one common column.
    if len(common_columns) == 1:
        key_column = common_columns.pop()
        print("The key column is: " + key_column)

    # If there is more than one common column, use the column with the most matching values as the key column.
    # Ignore the rest of the common columns.

    if len(common_columns) > 1:
        key_column = None
        max_matching_values = 0
        max_rows_to_check = 1000  # Define the maximum number of rows to check

        for column in common_columns:
            matching_values = 0

            # Limit the range to the first 1000 rows for both dataframes
            for i in range(min(len(df1), max_rows_to_check)):
                for j in range(min(len(df2), max_rows_to_check)):
                    if df1[column][i] == df2[column][j]:
                        matching_values += 1

            if matching_values > max_matching_values:
                max_matching_values = matching_values
                key_column = column

        print("The key column is: " + str(key_column))

        # If a key column is not found, return None.
        if key_column is None:
            return None

    # if len(common_columns) > 1:
    #     key_column = None
    #     max_matching_values = 0
    #
    #     for column in common_columns:
    #         matching_values = sum(
    #             1 for i in range(len(df1)) for j in range(len(df2)) if df1[column][i] == df2[column][j]
    #         )
    #
    #         if matching_values > max_matching_values:
    #             max_matching_values = matching_values
    #             key_column = column
    #
    #     print("The key column is: " + key_column)
    #
    #     # If a key column is not found, return None.
    #     if key_column is None:
    #         return None


    # else:
    #     # Use the common column with the most matching values as the key column.
    #     key_column = None
    #     max_matching_values = 0
    #     for column in common_columns:
    #         matching_values = 0
    #         for i in range(len(df1)):
    #             for j in range(len(df2)):
    #                 if df1[column][i] == df2[column][j]:
    #                     matching_values += 1
    #         if matching_values > max_matching_values:
    #             max_matching_values = matching_values
    #             key_column = column
    #             print("The key column is: " + key_column)
    #
    #     # If a key column was not found, return None.
    #     if key_column is None:
    #         return None

    # Calculate the percentage of matching values between the two dataframes.
    matching_values = 0
    for i in range(len(df1)):
        for j in range(len(df2)):
            if df1[key_column][i] == df2[key_column][j]:
                matching_values += 1
    percentage = (matching_values / len(df1)) * 100
    print("The percentage of matching values between the two dataframes is: " + str(percentage) + "%" )

    # Merge the dataframes.
    merged_df = pd.merge(df1, df2, on=key_column)
    # If there are duplicate rows in the merged dataframe under the key_column, drop them.
    if merged_df.duplicated(subset=key_column).any():
        merged_df.drop_duplicates(subset=key_column, inplace=True)

    # combine first and last name
    if ('firstname' in merged_df.columns or 'first_name' in merged_df.columns) and ('lastname' in merged_df.columns
                                                                                    or 'last_name' in merged_df.columns):
        if 'first_name' in merged_df.columns:
            merged_df.rename(columns={'first_name': 'firstname'}, inplace=True)
        if 'last_name' in merged_df.columns:
            merged_df.rename(columns={'last_name': 'lastname'}, inplace=True)

        merged_df['fullname'] = merged_df['firstname'] + " " + merged_df['lastname']
        merged_df.drop(columns=['firstname', 'lastname'], inplace=True)

    # Drop usernicename if username exists.
    if 'username' in merged_df.columns and 'usernicename' in merged_df.columns:
        merged_df.drop(columns=['usernicename'], inplace=True)

    output_dir = os.path.dirname(file1) + "/merged.csv"
    merged_df.to_csv(output_dir, index=False)
    if os.path.exists(output_dir):
        print("The dataframes have been merged successfully")

    if 'id' in merged_df.columns:
        merged_df.rename(columns={'id': 'userid'}, inplace=True)

    # Return the merged dataframe.
    return merged_df


if __name__ == "__main__":
    # Start the timer.
    start = timeit.default_timer()

    # Check if the user has provided two CSV files as command line arguments.
    if len(sys.argv) != 3:
        print("Please provide two CSV files as command line arguments")
        sys.exit()

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    merged_df = merge_csv_files(file1, file2)

    print(merged_df.head())

    # Stop the timer.
    stop = timeit.default_timer()
    print('Time: ', stop - start)

