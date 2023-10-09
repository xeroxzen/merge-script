import pandas as pd
import sys
import os
import time, timeit

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

    # Get a list of the common columns between the two dataframes.
    common_columns = set(df1.columns).intersection(df2.columns)

    # If there are no common columns, return None.
    if not common_columns:
        print("The dataframes do not have any common columns")
        return None

    else:
        # Use the common column with the most matching values as the key column.
        key_column = None
        max_matching_values = 0
        for column in common_columns:
            matching_values = 0
            for i in range(len(df1)):
                for j in range(len(df2)):
                    if df1[column][i] == df2[column][j]:
                        matching_values += 1
            if matching_values > max_matching_values:
                max_matching_values = matching_values
                key_column = column

        # If a key column was not found, return None.
        if key_column is None:
            return None

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
    # Check if there are duplicate rows in the merged dataframe. Use the userid column to see duplicates. If there are,
    # remove them and leave one that has more data.
    if 'userid' in merged_df.columns:
        merged_df = merged_df.drop_duplicates(subset='userid', keep='last')
        print("The duplicate rows have been removed")

    output_dir = os.path.dirname(file1) + "/merged.csv"
    merged_df.to_csv(output_dir, index=False)
    if os.path.exists(output_dir):
        print("The dataframes have been merged successfully")

    if 'id' in merged_df.columns:
        merged_df.rename(columns={'id': 'userid'}, inplace=True)

    # Return the merged dataframe.
    return merged_df

if __name__ == "__main__":
    # Check if the user has provided two CSV files as command line arguments.
    if len(sys.argv) != 3:
        print("Please provide two CSV files as command line arguments")
        sys.exit()

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    merged_df = merge_csv_files(file1, file2)

    print(merged_df.head())