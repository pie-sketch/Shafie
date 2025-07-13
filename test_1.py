#test_matching
import pandas as pd
import re
from rapidfuzz import fuzz
from collections import defaultdict
from time import perf_counter
import sys

# Keywords to recognize buildings
building_keywords = [
    # Residential
    "pangsapuri", "apartment", "kondominium", "flat", "rumah pangsa", "perumahan",
    "residensi", "residence", "soho", "suite", "rumah", "rumah teres",
    "rumah kos rendah", "rumah mampu milik", "ppr", "kuarters", "projek perumahan", "kondo",

    # Towers / High-Rises
    "menara", "tower", "blok", "blok a", "blok b", "blok c", "blok d", "blok e", "blok f",
    "blok g", "blok h", "blok i", "blok j", "blok k", "blok l", "blok m", "blok n",
    "parklane", "vista", "platinum", "axis", "empire", "citadines", "sky", "heights",
    "mont", "pinnacle", "galleria", "one", "two", "three", "quattro", "klcc", "i-city",

    # Government / Public
    "kuarters", "balai", "kompleks", "jabatan", "mahkamah", "pejabat", "masjid",
    "hospital", "klinik", "dewan", "institut", "pusat", "balai",

    # Commercial / Mixed-Use
    "mall", "plaza", "avenue", "square", "pavilion", "gateway", "court", "city",
    "sentral", "utama", "metro", "arcade", "galeri", "retail", "hub", "walk",
    "the", "dataran", "putra",

    # Hotel / Serviced Residence
    "hotel", "servis", "serviced", "inn", "lodge", "homestay", "guesthouse",
    "resort", "villa", "chalets", "bnb"
]

# Common tokens to ignore in scoring
common_tokens = {
    "kuala", "lumpur", "selangor", "malaysia", "my", "jalan", "jln", "kg", "tmn",
    "wp", "wilayah", "persekutuan"
}

# Helper functions
def clean_string(text):
    if not isinstance(text, str):
        return ""
    cleaned = (
        text.replace(",", " ").replace(".", " ").replace("_", " ")
        .replace(";", " ").replace("'", " ").replace("{", " ")
        .replace("}", " ").replace("[", " ").replace("]", " ")
        .replace("\\", " ").replace("?", " ").replace("!", " ")
        .replace("*", " ").replace(":", " ").replace("\n", " ")
        .replace("taman", "tmn").replace("lorong", "lrg")
        .replace("kampung", "kg").replace("jalan", "jln")
    )
    return remove_repetitive_words(cleaned.lower().strip())

def remove_repetitive_words(s):
    words = s.split()
    seen = set()
    return " ".join([w for w in words if not (w in seen or seen.add(w))])

def has_primary_token_overlap(tokens1, tokens2):
    return any(tok in tokens2 for tok in tokens1 if tok not in common_tokens and len(tok) > 2)

def has_building_keyword(text):
    text_tokens = set(text.lower().split())
    return any(set(kw.lower().split()) & text_tokens for kw in building_keywords)

def show_progress(current, total, start_time):
    percent = (current + 1) / total
    bar_len = 30
    filled_len = int(bar_len * percent)
    bar = '█' * filled_len + '-' * (bar_len - filled_len)
    elapsed = perf_counter() - start_time
    sys.stdout.write(f'\r⏳ [{bar}] {int(percent * 100)}%  ({current + 1}/{total}) — Elapsed: {round(elapsed, 1)}s')
    sys.stdout.flush()

# --- Remove duplicate postcode from end of matched key ---
def remove_duplicate_postcode(match_key, postcode):
    if not isinstance(match_key, str):
        return match_key
    parts = match_key.strip().split()
    if len(parts) >= 2 and parts[-1] == parts[-2] == postcode:
        return " ".join(parts[:-1])
    return match_key

def match_address_to_latlong(filepath):
    start_time = perf_counter()
    df_input = pd.read_excel(filepath, sheet_name='Input')
    df_nodes = pd.read_excel(filepath, sheet_name='Nodes')
    df_ref = pd.read_excel(filepath, sheet_name='Reference')

    for col in ["LL", "Matched Key", "Score"]:
        if col not in df_input.columns:
            df_input[col] = "" if col != "Score" else 0

    df_input["LL"] = df_input["LL"].astype("object")
    df_input["Matched Key"] = df_input["Matched Key"].astype("object")
    df_input["Score"] = df_input["Score"].astype("float")

    # Reference map
    reference_map = []
    for _, row in df_ref.iterrows():
        ref_key = clean_string(str(row["Address"]))
        ref_area = clean_string(str(row["Area"]))
        ref_postcode = str(row["Postcode"]).strip()
        ll = row["LL"]

        full_key = f"{ref_key} {ref_area}".strip()
        key_tokens = set(full_key.split())

        is_generic = (
            len(ref_key.split()) < 4 and
            not any(char.isdigit() for char in ref_key)
        )

        reference_map.append({
            "address": ref_key,
            "area": ref_area,
            "postcode": ref_postcode,
            "ll": ll,
            "tokens": key_tokens,
            "is_generic": is_generic
        })

    # Node map
    postcode_node_map = defaultdict(list)
    for _, row in df_nodes.iterrows():
        key = str(row.iloc[0] or "")
        postcode = str(row.iloc[1] or "")
        ll = row.iloc[2]
        cleaned_key = clean_string(key)
        tokens = set(cleaned_key.split())
        postcode_node_map[postcode].append({
            "key": key,
            "postcode": postcode,
            "ll": ll,
            "cleaned_key": cleaned_key,
            "tokens": tokens
        })

    total = len(df_input)

    for idx, row in df_input.iterrows():
        raw_address = str(row.get("full_address", ""))
        postcode = str(row.get("postcode", ""))
        cleaned_address = clean_string(raw_address)
        tokens_input = set(cleaned_address.split())

        best_match = None
        candidates = postcode_node_map.get(postcode, [])

        # Match from Nodes
        for node in candidates:
            if not has_primary_token_overlap(node["tokens"], tokens_input):
                continue

            jaccard = len(tokens_input & node["tokens"]) / len(node["tokens"] or [1])
            if jaccard < 0.75:
                continue

            ratio = fuzz.token_set_ratio(cleaned_address, node["cleaned_key"]) / 100
            if ratio < 0.75:
                continue

            boost = 0.10 if has_building_keyword(cleaned_address) and has_building_keyword(node["cleaned_key"]) else 0
            penalty = 0.05 if has_building_keyword(cleaned_address) != has_building_keyword(node["cleaned_key"]) else 0

            score = (0.7 * jaccard + 0.3 * ratio + boost - penalty) * 100

            if not best_match or score > best_match["score"]:
                best_match = {**node, "score": round(score)}

        if best_match and best_match["score"] >= 83:
            df_input.at[idx, "LL"] = best_match["ll"]
            df_input.at[idx, "Matched Key"] = f"{best_match['key']} {best_match['postcode']}"
            df_input.at[idx, "Score"] = best_match["score"]
            show_progress(idx, total, start_time)
            continue

        # Match from Reference (fallback)
        for ref in reference_map:
            if ref["is_generic"]:
                if not all(tok in tokens_input for tok in ref["address"].split()):
                    continue
                if not all(tok in tokens_input for tok in ref["area"].split()):
                    continue
            else:
                if len(tokens_input & ref["tokens"]) != len(ref["tokens"]):
                    continue

            full_key = f"{ref['address']} {ref['area']} {ref['postcode']}"
            df_input.at[idx, "LL"] = ref["ll"]
            df_input.at[idx, "Matched Key"] = full_key
            df_input.at[idx, "Score"] = 100
            break
        else:
            df_input.at[idx, "LL"] = ""
            df_input.at[idx, "Matched Key"] = ""
            df_input.at[idx, "Score"] = 0

        show_progress(idx, total, start_time)

    # Fix duplicate postcode in "Matched Key"
    df_input["Matched Key"] = df_input.apply(
        lambda x: remove_duplicate_postcode(x["Matched Key"], str(x.get("postcode", "")).strip()),
        axis=1
    )

    # Export
    out_path = "C:/Users/User/Desktop/testdata_match.csv"
    df_input.to_csv(out_path, index=False)
    total_time = round(perf_counter() - start_time, 2)
    match_count = df_input["LL"].astype(bool).sum()

    print(f"\n\n✅ Matching complete. File saved to: {out_path}")
    print(f"⏱️ Total runtime: {total_time} seconds")
    print(f"📌 Total matched: {match_count} / {total}")

# ✅ Run
match_address_to_latlong("C:/Users/User/Desktop/testdata.xlsx")
