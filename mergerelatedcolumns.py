"""
@author: Andile Mbele
@program: Merge CSV files that have related columns in common and have row values that match

Breaking down the problem:
1. Get all the CSV files in the directory
2. Read the CSV files into a dataframe
3. Compare the dataframes
4. Check for matching values in the rows
5. If the percentage of matching values is greater than 50%, merge the dataframes
6. Write the merged dataframe to a new CSV file
"""

import pandas as pd
import os
import glob
import csv
import json

def get_csv_files():
    """
    Get all the CSV files in the directory
    :return: list of CSV files
    """
    csv_files = []
    for file in glob.glob("*.csv"):
        csv_files.append(file)
    return csv_files

def read_csv_files(csv_files):
    """
    Read the CSV files into a dataframe
    :param csv_files: list of CSV files
    :return: list of dataframes
    """
    dataframes = []
    for file in csv_files:
        dataframes.append(pd.read_csv(file))
    return dataframes

def compare_dataframes(dataframes):
    """
    Compare the dataframes
    :param dataframes: list of dataframes
    :return: list of dataframes
    """
    for i in range(len(dataframes)):
        for j in range(len(dataframes)):
            if i != j:
                dataframes[i] = dataframes[i].merge(dataframes[j], on='id')
    return dataframes

