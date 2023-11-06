"""
@author: Andile Mbele
"""

import os
import sys


def find_session_csv_files(directory_path, skip_directories=None):
    session_csv_files = []

    for root, dirs, files in os.walk(directory_path):
        # Check if any of the skip directories are in the current path
        if any(skip_dir.lower() in root.lower() for skip_dir in skip_directories):
            continue

        for filename in files:
            if filename.lower().endswith('.csv') and 'session' in filename.lower():
                session_csv_files.append(os.path.join(root, filename))

    return session_csv_files


def main():
    directory_path = sys.argv[1]

    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist.")
        return

    skip_directories = ["badones", "bad ones",
                        "complete", "sql_statements", "unable_to_parse"]
    session_csv_files = find_session_csv_files(
        directory_path, skip_directories)

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
