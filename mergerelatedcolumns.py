"""
@author: Andile Mbele
@program: Merge CSV files that have related columns in common and have row values that match

Breaking down the problem:
1. Get two CSV files in the directory, the files can be passed as command line arguments or the program can prompt the user to enter the file names
2. Read the CSV files into a dataframe
3. Compare the dataframes to see if they have related columns in common. Use either the id, userid, username or email column as the key for comparison.
4. Check for matching values in the rows. Use email or username as the key for match search. Sometimes the email or username column may not be present in the CSV file, in that case use the id column.
5. If the percentage of matching values is greater than 50%, merge the dataframes. Use the id or userid column as the key.
6. Write the merged dataframe to a new CSV file
"""

import pandas as pd
import sys

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

    # Compare the dataframes to see if they have related columns in common. Use either the id, userid, username or email column as the key for comparison.
    if 'id' in df1.columns and 'id' in df2.columns:
        if df1['id'].equals(df2['id']):
            key = 'id'
    elif 'id' in df1.columns and 'userid' in df2.columns:
        if df1['id'].equals(df2['userid']):
            key = 'userid'
    elif 'userid' in df1.columns and 'id' in df2.columns:
        if df1['userid'].equals(df2['id']):
            key = 'id'
    elif 'userid' in df1.columns and 'userid' in df2.columns:
        if df1['userid'].equals(df2['userid']):
            key = 'userid'
    elif 'username' in df1.columns and 'username' in df2.columns:
        if df1['username'].equals(df2['username']):
            key = 'username'
    elif 'email' in df1.columns and 'email' in df2.columns:
        if df1['email'].equals(df2['email']):
            key = 'email'
    else:
        print("The dataframes do not have related columns in common")

    # for col in df1.columns:
    #     if col in df2.columns:
    #         key = col
    #         break

#     Check matching values in the rows. Find a secondary key to use for match search. Use email column or email
#     regex as the key for secondary match search.
    if 'email' in df1.columns and 'email' in df2.columns:
        if df1['email'].equals(df2['email']):
            secondary_key = 'email'
    else:
        secondary_key = 'id'

    # Calculate the percentage of matching values
    matching_values = 0
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
    if len(sys.argv) == 3:
        file1 = sys.argv[1]
        file2 = sys.argv[2]
        merge_csv_files(file1, file2)
    else:
        file1 = input("Enter the first CSV file name: ")
        file2 = input("Enter the second CSV file name: ")
        merge_csv_files(file1, file2)