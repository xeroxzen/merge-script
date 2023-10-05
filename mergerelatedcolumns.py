"""
@author: Andile Mbele
@program: Merge CSV files that have related columns in common and have row values that match

Breaking down the problem:
1. Get two CSV files in the directory, the files can be passed as command line arguments or the program can prompt the user to enter the file names
2. Read the CSV files into a dataframe
3. Compare the dataframes e.g
    i. if the first dataframe has 5 columns, check if the second dataframe has 5 columns as well.
    ii. Check if the columns have the same names. If the columns have different names, check if the columns have the
    same data type.
    iii. If the columns have different data types, check if the columns have the same number of rows.
4. Check for matching values in the rows. Use email or username as the key for match search. Sometimes the email or username column may not be present in the CSV file, in that case use the id column.
5. If the percentage of matching values is greater than 50%, merge the dataframes. Use the id or userid column as the key.
6. Write the merged dataframe to a new CSV file
"""

# Importing the required libraries
import pandas as pd
import sys

def merge_csv_files(file1, file2):
    """
    This function takes two CSV files as arguments and merges them if they have related columns and matching values in the rows.
    :param file1: The first CSV file
    :param file2: The second CSV file
    :return: A new CSV file with the merged dataframes
    """
    # Reading the CSV files into dataframes
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    if len(df1.columns) == len(df2.columns):
        if df1.columns.tolist() == df2.columns.tolist():
            if df1.dtypes.tolist() == df2.dtypes.tolist():
                if len(df1) == len(df2):
                    if df1.equals(df2):
                        merged_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
                        merged_df.to_csv('merged.csv', index=False)
                        print("The CSV files have been merged successfully!")
                    else:
                        print("The CSV files have different values in the rows!")
                else:
                    print("The CSV files have different number of rows!")
            else:
                print("The CSV files have different data types!")
        else:
            print("The CSV files have different column names!")
    else:
        print("The CSV files have different number of columns!")

if __name__ == "__main__":
    # Getting the CSV file names from the command line arguments
    file1 = input("Enter the first CSV file name: ", sys.argv[1])
    file2 = input("Enter the second CSV file name: ", sys.argv[2])
    # Calling the merge_csv_files function
    merge_csv_files(file1, file2)

