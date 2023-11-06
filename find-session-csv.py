"""_summary_
Problem:

    We need a script that can recursively search through a directory and its subdirectories, identifying CSV files whose filenames contain the word "session." This script will serve as the first step in the process of comparing session and user data.

Tasks:

    Create a Python script that recursively searches for CSV files with "session" in the filename.
    List the discovered CSV files.
    Create documentation on how to use the script.
"""

import os
import sys


def find_session_csv_files(directory_path):
    session_csv_files = []

    for root, _, files in os.walk(directory_path):
        for filename in files:
            if filename.lower().endswith('.csv') and 'session' in filename.lower():
                session_csv_files.append(os.path.join(root, filename))

    return session_csv_files


def main():
    directory_path = sys.argv[1]

    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist.")
        return

    session_csv_files = find_session_csv_files(directory_path)
    
    # Count total number of files found
    count = 0

    if session_csv_files:
        print("Session CSV files found:")
        for file in session_csv_files:
            count += 1
            print(file)
        print(f"Total files found: {count}")
    else:
        print("No session CSV files found in the directory and its subdirectories.")


if __name__ == "__main__":
    main()
