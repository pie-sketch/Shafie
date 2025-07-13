view_records.py
import sqlite3

# Path to your database
DB_PATH = "C:/Users/User/Desktop/core_may_to_july.db"
conn = sqlite3.connect(DB_PATH)

# Run a query to get the first 100 records
query = """
SELECT address, postcode, LL, data_date
FROM reference
WHERE postcode = '53100'
LIMIT 100
"""

rows = conn.execute(query).fetchall()
conn.close()

# Print the result
for i, row in enumerate(rows, 1):
    address, postcode, ll, data_date = row
    print(f"{i:03d}. 📍 Address: {address} | 🏷️ Postcode: {postcode} | 📌 LL: {ll} | 📅 Date: {data_date}")
