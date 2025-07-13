import pandas as pd
import re
from rapidfuzz import fuzz
from collections import defaultdict
from time import perf_counter
import os
import ast
import sys

start_time = None

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

def print_progress(current, total, start_time):
    percent = (current + 1) / total
    bar_len = 15
    filled_len = int(bar_len * percent)
    bar = '█' * filled_len + '-' * (bar_len - filled_len)
    elapsed = perf_counter() - start_time
    eta = (elapsed / (percent + 1e-5)) * (1 - percent)
    mins = int(elapsed // 60)
    secs = round(elapsed % 60)
    eta_m = int(eta // 60)
    eta_s = int(eta % 60)
    sys.stdout.write(
        f'\rProgress: {current + 1} / {total} ({int(percent * 100)}%)'
        f' | ⏱️ {mins:02d}:{secs:02d} | ETA: {eta_m:02d}:{eta_s:02d} | [{bar}]'
    )
    sys.stdout.flush()

def clean_address(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def clean_string(s):
    s = str(s).lower()
    s = re.sub(r"[^\w\s/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def has_primary_token_overlap(tokens1, tokens2):
    return any(tok in tokens2 for tok in tokens1 if tok not in common_tokens and len(tok) > 2)

def has_building_keyword(text):
    text_tokens = set(text.lower().split())
    return any(set(kw.lower().split()) & text_tokens for kw in building_keywords)

def remove_duplicate_postcode(text):
    return re.sub(r'(\b\d{5}\b)(\s+\1)+', r'\1', text)

def is_generic_match(key):
    try:
        toks = ast.literal_eval(key) if isinstance(key, str) else []
        toks = [t.lower() for t in toks if not t.isdigit()]
        return len(set(toks) & {"johor", "perak", "kedah", "selangor", "perlis", "sabah", "sarawak", "kl", "melaka", "penang"}) >= 1 and len(toks) <= 3
    except:
        return False

def run_tier1(df_input, postcode_node_map):
    results = []
    for idx, row in df_input.iterrows():
        raw_address = str(row.get("full_address", ""))
        postcode = str(row.get("postcode", ""))
        cleaned = clean_address(raw_address)
        tokens_input = set(cleaned.split())

        best = None
        for node in postcode_node_map.get(postcode, []):
            if not has_primary_token_overlap(node["tokens"], tokens_input):
                continue
            overlap = len(tokens_input & node["tokens"])
            jaccard = overlap / len(node["tokens"] or [1])
            if len(tokens_input) >= 7 and overlap < 5:
                continue
            if jaccard < 0.60:
                continue
            ratio = fuzz.token_set_ratio(cleaned, node["cleaned_key"]) / 100
            if ratio < 0.75:
                continue
            boost = 0.10 if has_building_keyword(cleaned) and has_building_keyword(node["cleaned_key"]) else 0
            penalty = 0.05 if has_building_keyword(cleaned) != has_building_keyword(node["cleaned_key"]) else 0
            score = ((0.7 * jaccard + 0.3 * ratio + boost - penalty) / 1.1) * 100
            if not best or score > best.get("score", 0):
                best = {
                    "ll": node["ll"],
                    "matched_key": remove_duplicate_postcode(f"{node['key']} {node['postcode']}"),
                    "score": round(score),
                    "source": "Nodes"
                }
        if best and best["score"] >= 90:
            results.append((best["ll"], best["matched_key"], best["score"], best["source"]))
        else:
            results.append(("", "", 0, ""))
        print_progress(idx, len(df_input), start_time)
    return results

def match_address_to_latlong(filepath):
    global start_time
    start_time = perf_counter()
    df_input = pd.read_excel(filepath, sheet_name='Input')
    df_nodes = pd.read_excel(filepath, sheet_name='Nodes')

    if "Matched Key" in df_input.columns:
        df_input = df_input[~df_input["Matched Key"].apply(is_generic_match)]

    for col in ["LL", "Matched Key", "Score", "Source"]:
        if col not in df_input.columns:
            df_input[col] = "" if col != "Score" else 0

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

    print("\n🔍 Running Tier 1 (Nodes)...")
    tier1_results = run_tier1(df_input, postcode_node_map)
    df_input[["LL", "Matched Key", "Score", "Source"]] = tier1_results

    df_input = df_input[df_input["Score"] >= 75]

    def format_key(value):
        if isinstance(value, str):
            try:
                parsed = ast.literal_eval(value)
                if isinstance(parsed, list):
                    return ", ".join(tok.capitalize() for tok in parsed)
            except:
                pass
        return value

    df_input["Matched Key"] = df_input["Matched Key"].apply(format_key)
    out_path = f"C:/Users/User/Desktop/{os.path.basename(filepath).replace('.xlsx', '')}_match.csv"
    df_input.to_csv(out_path, index=False)

    print(f"\n✅ Completed. Saved to: {out_path}")
    print(f"⏱️ Total Time: {int(perf_counter() - start_time)}s")
    print(f"✅ Total matched: {len(df_input)}")

if __name__ == "__main__":
    match_address_to_latlong("C:/Users/User/Desktop/testdata2.xlsx")
