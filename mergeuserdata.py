import os
import pandas as pd
import random
import string


def random_string(length=4):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


def merge_csv_files(directory):
    # Create an empty dataframe to store merged data
    merged_data = pd.DataFrame()

    # List the CSv files in the directory that have "users" and "usermeta" in th\eir filename
    csv_files = [file for file in os.listdir(directory) if file.endswith(
        ".csv") and ("users" in file or "usermeta" in file)]

    if len(csv_files) < 2:
        print("Insufficient files to proceed with merge.")
        return

    # Initialize the common_column variable
    key_column = None

    # Iterate over the CSV files
    for file in csv_files:
        file_path = os.path.join(directory, file)
        data = pd.read_csv(file_path, encoding="utf-8")

        # Determine the common column based on the file's name
        if "users" in file:
            key_column = "id"
        elif "usermeta" in file:
            key_column = "userid"

        # Check if the columns actually exist
        if key_column not in data.columns:
            print(
                f"Warning: The key column '{key_column}' is not present in the file '{file}' and will be skipped.")

        # Merge data
        merged_data = merged_data.merge(data, on=key_column, how="outer")

    if key_column is None:
        print("No suitable common column found in selected files.")
    else:
        merged_file_name = f"merged_{random_string()}.csv"
        merged_file_path = os.path.join(directory, merged_file_name)

        # Save the merged data to the new file
        merged_data.to_csv(merged_file_path, index=False)

        print(f"Merged data saved to {merged_file_name}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python merge_csv.py <directory_path>")
    else:
        directory_path = sys.argv[1]
        if not os.path.exists(directory_path):
            print("Directory does not exist")
        else:
            merge_csv_files(directory_path)
