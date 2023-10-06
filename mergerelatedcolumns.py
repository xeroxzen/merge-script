import pandas as pd
import sys
import os


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

    # If there are no common columns, check if the two files have the words "user" or "usermeta" in their file names.
    if not common_columns:
        if ('user' in file1 or 'usermeta' in file1) and ('user' in file2 or 'usermeta' in file2):
            # If so, set the key column to "id" or "userid", whichever is present in both dataframes.
            if 'id' in df1.columns and 'userid' in df2.columns:
                key_column = 'id'
            elif 'userid' in df1.columns and 'id' in df2.columns:
                key_column = 'userid'
            else:
                print("Could not find a key column for merging the dataframes.")
                return None
        else:
            print(
                "The dataframes do not have any common columns and do not have the words 'user' or 'usermeta' in their file names.")
            return None
    else:
        # Check if "id" and "userid" are both present in the common columns, and choose one as the key column.
        if 'id' in common_columns and 'userid' in common_columns:
            key_column = 'id'  # You can choose 'userid' here if you prefer.
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
    print("The percentage of matching values is: " + str(percentage) + "%")

    # If the percentage of matching values is greater than 50%, merge the dataframes.
    if percentage > 50:
        output_dir = os.path.dirname(file1) + "/merged.csv"
        merged_df = pd.merge(df1, df2, on=key_column)
        merged_df = merged_df.drop_duplicates()
        # Rename 'id' column to 'userid' in the merged dataframe.
        if 'id' in merged_df.columns:
            merged_df.rename(columns={'id': 'userid'}, inplace=True)
        merged_df.to_csv(output_dir, index=False)
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
