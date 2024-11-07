import pandas as pd
import mysql.connector
import os

# MySQL connection
def connect_to_mysql():
    return mysql.connector.connect(
        host="host",
        user="user",
        password="password",
        database=None
    )

# Create a new schema in the database
def create_schema(cursor, schema_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`")
    cursor.execute(f"USE `{schema_name}`")

# Make column names unique
def make_unique_columns(columns):
    unique_columns = []
    counts = {}
    for col in columns:
        col = col.strip().replace(" ", "_")
        if col in counts:
            counts[col] += 1
            unique_columns.append(f"{col}_{counts[col]}")
        else:
            counts[col] = 0
            unique_columns.append(col)
    return unique_columns

# Create a table for each sheet in Excel and insert data
def create_table_from_sheet(cursor, df, table_name, file_name, sheet_name):
    # Generate unique column names
    columns = make_unique_columns(df.columns)
    columns_with_types = ", ".join([f"`{col}` TEXT" for col in columns])

    # Create table
    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_with_types})")

    # Insert data into table
    for i, row in df.iterrows():
        values = []
        for v in row.tolist():
            if pd.isna(v):
                values.append("NULL")
            elif isinstance(v, (int, float)):
                values.append(str(v))
            else:
                values.append(f"'{str(v).replace('\'', '\\\'')}'")

        query = f"INSERT INTO `{table_name}` VALUES ({', '.join(values)})"
        try:
            cursor.execute(query)
        except mysql.connector.errors.ProgrammingError as e:
            print(f"Error: {e}, File: '{file_name}', Sheet: '{sheet_name}', Row: {i} skipped")

# Process the Excel file
def process_excel_to_mysql(file_path):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    excel_data = pd.ExcelFile(file_path)
    schema_name = os.path.splitext(os.path.basename(file_path))[0]

    create_schema(cursor, schema_name)

    for sheet in excel_data.sheet_names:
        # Clean spaces in sheet name
        cleaned_sheet_name = sheet.strip()
        df = excel_data.parse(sheet)
        if df.empty:
            print(f"Warning: '{cleaned_sheet_name}' sheet is empty, table not created.")
            continue
        create_table_from_sheet(cursor, df, cleaned_sheet_name, file_path, cleaned_sheet_name)

    conn.commit()
    cursor.close()
    conn.close()

# Folder containing Excel files
folder_path = "your path excel folder"
excel_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.xlsx')]

for file in excel_files:
    try:
        process_excel_to_mysql(file)
        print(f"'{file}' file successfully uploaded!")
    except Exception as e:
        print(f"General error: {e}, File: '{file}' could not be uploaded.")

print("Excel files upload to MySQL database completed!")
