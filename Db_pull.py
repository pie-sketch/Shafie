import sqlite3
import pandas as pd

DB_PATH = "C:/Users/User/Desktop/Core_2025.sqlite"
RECENT_MONTHS = ('202504', '202505', '202506', '202507')

def inspect_monthly_data(db_path, recent_months):
    conn = sqlite3.connect(db_path)
    
    for month in recent_months:
        print(f"\n📅 Month: {month}")
        
        # Count records
        count_query = "SELECT COUNT(*) FROM data_2025 WHERE data_date = ?"
        cursor = conn.execute(count_query, (month,))
        count = cursor.fetchone()[0]
        print(f"🔢 Total Rows: {count}")
        
        # Preview top 3 rows
        preview_query = """
            SELECT split, postcode, LL, data_date
            FROM data_2025
            WHERE data_date = ?
            LIMIT 3
        """
        df_preview = pd.read_sql_query(preview_query, conn, params=(month,))
        print("🔍 Top 3 Preview:")
        print(df_preview.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    inspect_monthly_data(DB_PATH, RECENT_MONTHS)

# 202507 = 1089894
# 202506 = 3435568
# 202505 = 3442930
# 202504 = 3926225
# total data used = 11894617

##🧾 Columns in 'data_core selected extract
# postcode
# LL
# data_date
# split