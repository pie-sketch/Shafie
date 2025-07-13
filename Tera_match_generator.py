import sqlite3
import pandas as pd
import re
from time import perf_counter

# --- Start Timer ---
start_time = perf_counter()

# --- Precompiled Regex ---
non_word_re = re.compile(r"[^\w\s/]")
multi_space_re = re.compile(r"\s+")
duplicate_postcode_re = re.compile(r'(\b\d{5}\b)(\s+\1)+')

# --- Utility Functions ---
def clean_string(s):
    s = str(s).lower()
    s = non_word_re.sub(" ", s)
    s = multi_space_re.sub(" ", s)
    return s.strip()

def remove_duplicate_postcode(text):
    return duplicate_postcode_re.sub(r'\1', text)

def jaccard_similarity(set1, set2):
    intersection = set1 & set2
    union = set1 | set2
    return len(intersection) / len(union) if union else 0.0

# --- Load input Excel ---
input_path = "C:/Users/User/Desktop/Tera.xlsx"
df = pd.read_excel(input_path)

# --- Connect to SQLite DB ---
conn = sqlite3.connect("C:/Users/User/Desktop/Core.sqlite")
cursor = conn.cursor()

# --- Process rows ---
results = []

for _, row in df.iterrows():
    full_address = str(row["full_address"])
    postcode = str(row["postcode"]).strip()

    if not postcode or not full_address:
        results.append(("", "", 0))
        continue

    cleaned_input = clean_string(remove_duplicate_postcode(full_address))
    input_tokens = set(cleaned_input.split())

    cursor.execute("""
        SELECT LL, split FROM data_df WHERE postcode = ?
    """, (postcode,))
    candidates = cursor.fetchall()

    best = None
    best_score = 0

    for ll, split_raw in candidates:
        if not split_raw:
            continue
        try:
            tokens = set(eval(split_raw)) if isinstance(split_raw, str) else set(split_raw)
        except:
            continue

        score = jaccard_similarity(input_tokens, tokens)
        if score >= 0.70 and score > best_score:
            best_score = score
            best = (ll, " ".join(sorted(tokens)), round(score * 100, 2))

    results.append(best if best else ("", "", 0))

# --- Save results to DataFrame ---
df["LL"] = [r[0] for r in results]
df["Match"] = [r[1] for r in results]
df["Score"] = [r[2] for r in results]

# --- Output Excel ---
output_path = "C:/Users/User/Desktop/Tera_match.xlsx"
df.to_excel(output_path, index=False)

# --- Done ---
end_time = perf_counter()
print(f"\n✅ Matching complete. Saved to {output_path}")
print(f"⏱️ Execution Time: {end_time - start_time:.2f} seconds")
