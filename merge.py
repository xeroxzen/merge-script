import pandas as pd
import os
import sys
import timeit


def merge_csv_files_in_directory(directory):
	# Create an empty dictionary to track dataframes by their filenames.
	dataframes = {}

	# List of possible PII.
	allowed_pii_columns = ['email', 'phone', 'ssn', 'user_id', 'customer_id', 'userid', 'customer_email', 'username',
						   'phonenumber', 'phone_number', 'billing_phone', 'billing_phone_number',
						   'billing_phone_number', 'billing_email', 'billing_email_address', 'billing_emailaddress',
						   'shipping_phone', 'shipping_phone_number', 'shipping_email', 'shipping_email_address',
						   'address', 'billing_address', 'shipping_address', 'address1', 'address2', 'address3']

	# Iterate through all files in the directory.
	for filename in os.listdir(directory):
		if filename.endswith(".csv"):
			file_path = os.path.join(directory, filename)
			df = pd.read_csv(file_path, encoding='utf-8')

			# Identify common columns with allowed PII columns.
			common_columns = set(df.columns).intersection(allowed_pii_columns)

			if not common_columns:
				print(f"No common columns with allowed PII in {filename}. Skipping.")
				continue

			# Use the first common column as the key column for merging.
			key_column = common_columns.pop()
			print(f"Using {key_column} as the key column for {filename}.")

			# Store the dataframe in the dictionary for later merging.
			dataframes[filename] = (df, key_column)

	# Check if there are dataframes that can be merged.
	if len(dataframes) < 2:
		print("Not enough dataframes to merge.")
		return

	# Merge dataframes one by one.
	merged_df = None
	for filename, (df, key_column) in dataframes.items():
		if merged_df is None:
			merged_df = df
		else:
			merged_df = pd.merge(merged_df, df, on=key_column)

	if merged_df.duplicated(subset=key_column).any():
		merged_df.drop_duplicates(subset=key_column, inplace=True)

	# combine first and last name
	if ('firstname' in merged_df.columns or 'first_name' in merged_df.columns) and ('lastname' in merged_df.columns
																					or 'last_name' in merged_df.columns):
		if 'first_name' in merged_df.columns:
			merged_df.rename(columns={'first_name': 'firstname'}, inplace=True)
		if 'last_name' in merged_df.columns:
			merged_df.rename(columns={'last_name': 'lastname'}, inplace=True)

		merged_df['fullname'] = merged_df['firstname'] + " " + merged_df['lastname']
		merged_df.drop(columns=['firstname', 'lastname'], inplace=True)

	# Drop usernicename if username exists.
	if 'username' in merged_df.columns and 'usernicename' in merged_df.columns:
		merged_df.drop(columns=['usernicename'], inplace=True)

	# Save the merged dataframe to a CSV file.
	output_dir = os.path.join(directory, "merged.csv")
	merged_df.to_csv(output_dir, index=False)

	if os.path.exists(output_dir):
		print("Merged dataframes successfully and saved as 'merged.csv'.")
		print(merged_df.head())


if __name__ == "__main__":
	# Start the timer.
	start = timeit.default_timer()

	# Check if the user provided a directory as a command line argument.
	if len(sys.argv) != 2:
		print("Please provide a directory containing CSV files as a command line argument.")
		sys.exit()

	directory = sys.argv[1]

	if not os.path.isdir(directory):
		print(f"'{directory}' is not a valid directory.")
		sys.exit()

	merge_csv_files_in_directory(directory)

	# Stop the timer.
	stop = timeit.default_timer()
	print('Time: ', stop - start)
