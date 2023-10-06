import pandas as pd
import sys


def merge_csv_files(file1, file2):
    """
    Merges two CSV files that have related columns in common and have row values that match.

    Args:
        file1: The first CSV file.
        file2: The second CSV file.

    Returns:
        A merged CSV file.
    """

    # Read the CSV files into Pandas dataframes.
    df1 = pd.read_csv(file1, encoding='utf-8')
    df2 = pd.read_csv(file2, encoding='utf-8')

    # Get a list of the common columns between the two dataframes.
    common_columns = set(df1.columns).intersection(df2.columns)

    # Check if the two dataframes have any common columns.
    if not common_columns:
        print("The dataframes do not have any common columns.")
        return None

    # Find a key column to use for merging the dataframes.
    key_column = None
    for column in common_columns:
        if df1[column].equals(df2[column]):
            key_column = column
            break

    # If a key column was not found, print an error message and return None.
    if key_column is None:
        print("Could not find a key column for merging the dataframes.")
        return None

    # Calculate the percentage of matching values between the two dataframes.
    matching_values = 0
    for i in range(len(df1)):
        for j in range(len(df2)):
            if df1[key_column][i] == df2[key_column][j]:
                matching_values += 1
    percentage = (matching_values / len(df1)) * 100

    # If the percentage of matching values is greater than 50%, merge the dataframes.
    if percentage > 50:
        merged_df = pd.merge(df1, df2, on=key_column)
        merged_df.to_csv('merged.csv', index=False)
        print("The dataframes have been merged successfully.")
    else:
        print("The percentage of matching values is less than 50%. Skipping merge.")

    return merged_df


if __name__ == '__main__':
    # Get the two CSV file names from the command line arguments.
    if len(sys.argv) == 3:
        file1 = sys.argv[1]
        file2 = sys.argv[2]
    else:
        print("Usage: merge_csv_files.py <file1> <file2>")
        exit(1)

    # Merge the two CSV files.
    merge_csv_files(file1, file2)