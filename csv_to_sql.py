import sqlite3
import pandas as pd

# Read CSV file into a DataFrame
csv_file_path = '.csv' # UPDATE THIS BEFORE EACH ENTRY !!!
df = pd.read_csv(csv_file_path)

# Connect to SQLite database
database_path = 'db.sql'
conn = sqlite3.connect(database_path)

table_name = input("Input the table name: ")

# Write DataFrame to SQLite database
df.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the database connection
conn.close()
