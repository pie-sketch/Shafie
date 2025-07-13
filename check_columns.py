import pandas as pd

df = pd.read_csv("task_data.csv")
print("🧾 Columns from CSV:")
for col in df.columns:
    print(f"- {repr(col)}")
