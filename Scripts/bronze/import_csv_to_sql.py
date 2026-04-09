# Alternative method since LOAD FILE in MySQL Workbench was not working properly 

import mysql.connector
import csv

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Sentinel1502@",
    database="DataWarehouse"
)

cursor = connection.cursor()

# Path to your CSV file
csv_file_path = "./sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv"

rows = []

# Read data from CSV and prepare for insertion (Update column names as per table schema)
query = """
INSERT INTO bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
VALUES (%s, %s, %s, %s)
"""

with open(csv_file_path, "r") as file:
    reader = csv.reader(file)
    next(reader)

    rows = []

    for row in reader:
        # Convert empty values to None
        cleaned_row = [None if x == "" else x for x in row]
        rows.append(tuple(cleaned_row))

cursor.executemany(query, rows)

connection.commit()

print("Batch import completed!")

cursor.close()
connection.close()
