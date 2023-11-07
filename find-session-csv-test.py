import os
import sys
import pandas as pd


def find_common_columns(session_csv, user_csv):
    session_df = pd.read_csv(session_csv)
    user_df = pd.read_csv(user_csv)

    common_columns = list(set(session_df.columns) & set(user_df.columns))
    
    print(session_df.head())
    print(user_df.head())

    print(
        f"Common columns between {session_csv} and {user_csv}: {common_columns}")
    return common_columns

def find_session_and_user_csv_files(directory_path):

    session_csv_files = []
    user_csv_files = []

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        if os.path.isfile(file_path) and filename.endswith('.csv'):
            if 'session' in filename.lower():
                session_csv_files.append(file_path)
            elif 'user' in filename.lower():
                user_csv_files.append(file_path)

    return session_csv_files, user_csv_files


def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py /path/to/directory")
        return

    directory_path = sys.argv[1]

    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist.")
        return

    session_csv_files, user_csv_files = find_session_and_user_csv_files(
        directory_path)

    common_columns = set()

    for session_csv in session_csv_files:
        session_directory = os.path.dirname(session_csv)
        user_csv = os.path.join(session_directory, "user.csv")

        if user_csv in user_csv_files:
            common_columns.update(find_common_columns(session_csv, user_csv))

    if common_columns:
        print("Common columns between session and user CSV files:")
        for column in common_columns:
            print(column)
    else:
        print("No common columns found between session and user CSV files.")


if __name__ == "__main__":
    main()
