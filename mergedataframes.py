import pandas as pd
import os
import sys
import timeit
import random
import string


def merge_csv_files_in_directory(directory):
	# Create a dictionary to track merged dataframes by key column.
	merged_dataframes = {}

	# List of possible PII.
	allowed_pii_columns = ['email', 'phone', 'ssn', 'user_id', 'customer_id', 'userid', 'customer_email', 'username',
						   'phonenumber', 'phone_number', 'billing_phone', 'billing_phone_number',
						   'billing_phone_number', 'billing_email', 'billing_email_address', 'billing_emailaddress',
						   'shipping_phone', 'shipping_phone_number', 'shipping_email', 'shipping_email_address',
						   'address', 'billing_address', 'shipping_address', 'address1', 'address2', 'address3']

	# Number of rows to consider for matching.
	max_rows_to_compare = 1000

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

			# Iterate through each common column as a potential key column.
			for key_column in common_columns:
				if key_column not in merged_dataframes:
					merged_dataframes[key_column] = df
				else:
					# Check if the data types of the key columns match. If not, use pd.concat.
					if df[key_column].dtype == merged_dataframes[key_column][key_column].dtype:
						# Limit comparison to the first max_rows_to_compare rows.
						merged_df = pd.merge(merged_dataframes[key_column][:max_rows_to_compare],
											 df[:max_rows_to_compare], on=key_column)
						merged_dataframes[key_column] = pd.concat([merged_dataframes[key_column], merged_df, df])
					else:
						merged_dataframes[key_column] = pd.concat([merged_dataframes[key_column], df])

	# Save merged dataframes and handle duplicates.
	for key_column, merged_df in merged_dataframes.items():
		# Combine 'firstname' and 'lastname' into 'fullname' if they exist.
		if 'firstname' in merged_df.columns and 'lastname' in merged_df.columns:
			merged_df['fullname'] = merged_df['firstname'] + " " + merged_df['lastname']
			merged_df.drop(columns=['firstname', 'lastname'], inplace=True)

		# Remove duplicates.
		if merged_df.duplicated().any():
			merged_df.drop_duplicates(inplace=True)

		# Generate a random number and add it to the output filename.
		random_suffix = ''.join(random.choice(string.ascii_letters) for _ in range(4))
		output_filename = f"merged_{random_suffix}_{key_column}.csv"
		output_dir = os.path.join(directory, output_filename)
		merged_df.to_csv(output_dir, index=False)

		if os.path.exists(output_dir):
			print(f"Merged dataframes using '{key_column}' as the key column and saved as '{output_filename}'.")
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
