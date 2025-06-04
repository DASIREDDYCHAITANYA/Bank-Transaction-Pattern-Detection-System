#Preprocessing CustomerImportance

f=pd.read_csv("/content/drive/MyDrive/CustomerImportance.csv")
# Example: count unique values in the 'customerId' column
# Use pandas methods for selecting and counting unique values
unique_count = f.loc[:, "fraud"].nunique()
print(f"Number of unique customerIds: {unique_count}")
# Drop rows where 'fraud' column is equal to 1
f = f[f['fraud'] != 1]
print(f"Shape of DataFrame after dropping rows with fraud=1: {f.shape}")
f.drop("fraud", axis=1, inplace=True)
f
# Save the modified DataFrame v to a new CSV file
output_csv_path = '/content/drive/MyDrive/customerimportance_processed.csv' # Specify your desired output path
f.to_csv(output_csv_path, index=False) # index=False prevents writing the DataFrame index as a column
print(f"Processed data saved to: {output_csv_path}")

#Preprocessing CustomerImportance

v='/content/drive/MyDrive/transactions.csv'
v = pd.read_csv(v)
unique_count = v.loc[:, "fraud"].nunique()
print(f"Number of unique customerIds: {unique_count}")
# Drop rows where 'fraud' column is equal to 1
v = v[v['fraud'] != 1]
print(f"Shape of DataFrame after dropping rows with fraud=1: {v.shape}")
# Save the modified DataFrame v to a new CSV file
output_csv_path = '/content/drive/MyDrive/transactions_processed.csv' # Specify your desired output path
v.to_csv(output_csv_path, index=False) # index=False prevents writing the DataFrame index as a column
print(f"Processed data saved to: {output_csv_path}")
