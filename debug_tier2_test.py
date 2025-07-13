import pandas as pd
import sqlite3
import re
from rapidfuzz import fuzz
import time
import sys
import os
from multiprocessing import Pool, Manager
from functools import partial
from threading import Thread, Event

# --- Configuration ---
DEBUG = False
DB_PATH = "C:/Users/User/Desktop/Core_2025.sqlite"
RECENT_MONTHS = ('202507','202506','202505','202504',)
NUM_WORKERS = 4
stop_event = Event()

# --- Thresholds ---
OVERLAP_THRESHOLD = 6 # ori used 5
FUZZY_THRESHOLD = 0.75
JACCARD_THRESHOLD = 0.7
SCORE_THRESHOLD = 87  # Tuning for better result

# --- Keywords ---
building_keywords = [
    "pangsapuri", "apartment", "kondominium", "flat", "rumah pangsa", "perumahan",
    "residensi", "residence", "soho", "suite", "rumah", "rumah teres",
    "rumah kos rendah", "rumah mampu milik", "ppr", "kuarters", "projek perumahan", "kondo",
    "menara", "tower", "blok", "parklane", "vista", "platinum", "axis", "empire",
    "citadines", "sky", "heights", "mont", "pinnacle", "galleria", "one", "two", "three",
    "quattro", "klcc", "i-city", "balai", "kompleks", "jabatan", "mahkamah", "pejabat", "masjid",
    "hospital", "klinik", "dewan", "institut", "pusat", "mall", "plaza", "avenue", "square",
    "pavilion", "gateway", "court", "city", "sentral", "utama", "metro", "arcade", "galeri",
    "retail", "hub", "walk", "the", "dataran", "putra", "hotel", "servis", "serviced", "inn",
    "lodge", "homestay", "guesthouse", "resort", "villa", "chalets", "bnb"
]

common_tokens = {
    "kuala", "lumpur", "selangor", "malaysia", "wp", "wilayah", "persekutuan",
    "jalanraya", "lebuhraya", "highway", "tingkatan", "blkg", "hadapan", "atas",
    "bawah", "hujung", "tepi", "berhampiran", "berdekatan", "bersebelahan", "pobox",
    "kuching", "sarawak"
}

# --- Utility Functions ---

def clean_string(s):
    s = str(s).lower()
    s = re.sub(r"[^\w\s/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def has_building_keyword(text):
    text_tokens = set(text.lower().split())
    return any(set(kw.lower().split()) & text_tokens for kw in building_keywords)

def remove_duplicate_postcode(text):
    return re.sub(r'(\b\d{5}\b)(\s+\1)+', r'\1', text)

def fetch_candidates(postcode, limit=12000): # tuning this affecting result - Ori used 10k
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    cursor = conn.cursor()
    results = []
    total = 0
    for month in RECENT_MONTHS:
        cursor.execute("""
            SELECT split, postcode, LL FROM data_2025
            WHERE postcode = ? AND data_date = ?
            LIMIT ?
        """, (postcode, month, limit - total))
        rows = cursor.fetchall()
        results.extend(rows)
        total += len(rows)
        if total >= limit:
            break
    conn.close()
    return results

def print_progress(shared_counter, total, start_time):
    while not stop_event.is_set():
        current = shared_counter.value
        bar_len = 40
        filled_len = int(round(bar_len * current / float(total)))
        bar = '█' * filled_len + '-' * (bar_len - filled_len)
        percent = round(100.0 * current / float(total), 1)
        elapsed = time.time() - start_time
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        sys.stdout.write(f"\r⏱️ [{bar}] {current}/{total} | {percent}% | {mins}:{secs:02d}")
        sys.stdout.flush()
        if current >= total:
            break
        time.sleep(0.5)

# --- Matching Logic ---

def process_chunk(chunk, shared_counter):
    results = []
    postcode_cache = {}

    for idx, row in chunk.iterrows():  # ✅ Include original index
        raw_address = str(row["full_address"])
        input_postcode = str(row["postcode"]).strip()

        cleaned = clean_string(raw_address)
        input_tokens = set(cleaned.split())
        eff_input = {t for t in input_tokens if t not in common_tokens}

        if input_postcode in postcode_cache:
            cand_rows = postcode_cache[input_postcode]
        else:
            cand_rows = fetch_candidates(input_postcode)
            postcode_cache[input_postcode] = cand_rows

        if not cand_rows:
            results.append((idx, {"LL": "", "Matched Key": "", "Score": 0}))  # ✅ Indexed
            shared_counter.value += 1
            continue  # ✅ Continue instead of return

        pre_candidates = []
        for split_text, pc, ll in cand_rows:
            if pc != input_postcode:
                continue
            cleaned_cand = clean_string(split_text)
            cand_tokens = set(cleaned_cand.split())
            eff_cand = {t for t in cand_tokens if t not in common_tokens}
            overlap = input_tokens & cand_tokens
            if len(overlap) < OVERLAP_THRESHOLD:
                continue
            pre_candidates.append((split_text, pc, ll, cleaned_cand, cand_tokens, eff_cand))

        candidates = []
        for split_text, pc, ll, cleaned_cand, cand_tokens, eff_cand in pre_candidates:
            overlap = input_tokens & cand_tokens
            overlap_eff = eff_input & eff_cand
            jaccard = len(overlap) / len(input_tokens | cand_tokens or [1])
            ratio = fuzz.token_set_ratio(cleaned, cleaned_cand) / 100

            if len(input_tokens) >= 6 and len(overlap) < OVERLAP_THRESHOLD:
                continue
            if ratio < FUZZY_THRESHOLD:
                continue
            if jaccard < JACCARD_THRESHOLD and len(overlap_eff) < 3:
                continue
            if len(input_tokens) < 6 and not has_building_keyword(cleaned):
                continue

            boost = 0.10 if has_building_keyword(cleaned) and has_building_keyword(cleaned_cand) else 0
            penalty = 0.05 if has_building_keyword(cleaned) != has_building_keyword(cleaned_cand) else 0

            score = ((0.3 * jaccard + 0.7 * ratio) if jaccard < 0.6 else (0.6 * jaccard + 0.4 * ratio))
            score = (score + boost - penalty) * 100
            score = min(score, 100)

            candidates.append((score, ll, split_text, pc))

        if candidates:
            best = max(candidates, key=lambda x: x[0])
            score, ll, raw_split, pc = best
            if score >= SCORE_THRESHOLD:
                cleaned_match = clean_string(raw_split)
                cleaned_match = remove_duplicate_postcode(f"{cleaned_match} {pc}")
                result = {
                    "LL": ll,
                    "Matched Key": cleaned_match,
                    "Score": round(score)
                }
            else:
                result = {
                    "LL": "",
                    "Matched Key": "",
                    "Score": 0
                }
        else:
            result = {
                "LL": "",
                "Matched Key": "",
                "Score": 0
            }


        results.append((idx, result))  # ✅ Include index for proper alignment
        shared_counter.value += 1

    return results


# --- Main Function ---

def debug_tier2_on_sample(filepath="C:/Users/User/Desktop/tier2_start.xlsx"):
    df = pd.read_csv(filepath) if filepath.lower().endswith(".csv") else pd.read_excel(filepath)
    assert "full_address" in df.columns and "postcode" in df.columns, "Missing required columns"

    manager = Manager()
    shared_counter = manager.Value("i", 0)
    start_time = time.time()
    chunks = [df.iloc[i::NUM_WORKERS] for i in range(NUM_WORKERS)]

    with Pool(processes=NUM_WORKERS) as pool:
        progress_thread = Thread(target=print_progress, args=(shared_counter, len(df), start_time))
        progress_thread.start()

        func = partial(process_chunk, shared_counter=shared_counter)
        all_results = pool.map(func, chunks)
        stop_event.set()
        progress_thread.join()

    # Flatten and re-index properly
    result_dict = {idx: res for chunk in all_results for idx, res in chunk}
    results_df = pd.DataFrame.from_dict(result_dict, orient="index").sort_index()

    df_result = df.copy()
    df_result["LL"] = results_df["LL"].values
    df_result["Matched Key"] = results_df["Matched Key"].values
    df_result["%"] = results_df["Score"].values

    output_path = os.path.join(os.path.expanduser("~"), "Desktop", "tier_2_match.csv")
    df_result.to_csv(output_path, index=False)

    matched_count = sum(1 for score in results_df["Score"] if score >= SCORE_THRESHOLD)
    total_count = len(df)
    mins = int((time.time() - start_time) // 60)
    secs = int((time.time() - start_time) % 60)

    print(f"\n\n Matched: {matched_count}/{total_count} | {round((matched_count/total_count)*100, 1)}% | Time: {mins}:{secs:02d}")
    print(f"✅ Debug CSV saved as {output_path}")


# --- Run ---
if __name__ == "__main__":
    debug_tier2_on_sample("C:/Users/User/Desktop/tier2_start.xlsx")

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
