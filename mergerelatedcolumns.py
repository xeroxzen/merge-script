import pandas as pd
import argparse

def merge_csv_files(file1, file2):
    """
    This function merges two CSV files that have related columns in common and have row values that match
    :param file1: CSV file 1
    :param file2: CSV file 2
    :return: merged CSV file
    """
    # Read the CSV files into a dataframe
    df1 = pd.read_csv(file1, encoding='utf-8')
    df2 = pd.read_csv(file2, encoding='utf-8')

    # Compare the dataframes to see if they have related columns in common.
    # Use either the id, userid, username or email column as the key for comparison.
    key = None
    for col in df1.columns:
        if col in df2.columns:
            key = col
            break

    # Check matching values in the rows. Find a secondary key to use for match search. Use email column or email
    # regex as the key for secondary match search.
    secondary_key = None
    if 'email' in df1.columns and 'email' in df2.columns:
        secondary_key = 'email'
    else:
        secondary_key = 'id'

    # Calculate the percentage of matching values
    matching_values = 0
    if key is not None and secondary_key is not None:
        for i in range(len(df1)):
            for j in range(len(df2)):
                if df1[key][i] == df2[secondary_key][j]:
                    matching_values += 1
        percentage = (matching_values / len(df1)) * 100

        # If the percentage of matching values is greater than 50%, merge the dataframes. Use the id or userid column as
        # the key.
        if percentage > 50:
            merged_df = pd.merge(df1, df2, on=key)
            merged_df.to_csv('merged.csv', index=False)
            print("The dataframes have been merged successfully")

if __name__ == '__main__':
    # Get two CSV files in the directory, the files can be passed as command line arguments or the program can prompt the user to enter the file names
    parser = argparse.ArgumentParser(description='Merge CSV files')
    parser.add_argument('--dir', type=str, help='Directory containing the CSV files')
    parser.add_argument('--f', type=str, action='append', help='CSV file path')
    args = parser.parse_args()

    # If the directory argument is passed, merge all CSV files in the directory
    if args.dir:
        import os
        csv_files = [os.path.join(args.dir, f) for f in os.listdir(args.dir) if f.endswith('.csv')]
        for i in range(len(csv_files) - 1):
            merge_csv_files(csv_files[i], csv_files[i + 1])
    # If the file argument is passed, merge the specified CSV files
    elif args.f:
        if len(args.f) != 2:
            raise ValueError('Must pass two CSV files as arguments')
        merge_csv_files(args.f[0], args.f[1])
    # Otherwise, prompt the user to enter the file names
    else:
        file1 = input("Enter the first CSV file name: ")
        file2 = input("Enter the second CSV file name: ")
        merge_csv_files(file1, file2)
