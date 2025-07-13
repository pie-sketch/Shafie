import pandas as pd
import re
from rapidfuzz import fuzz
from time import perf_counter
from collections import Counter
from datetime import timedelta

try:
    from tqdm import tqdm
    use_tqdm = True
except ImportError:
    use_tqdm = False

common_tokens = {
    "kuala", "lumpur", "selangor", "malaysia", "my", "jalan", "jln", "kg", "tmn", "wp", "wilayah", "persekutuan"
}

area_keywords = [
    "setapak", "wangsa maju", "keramat", "ampang", "melawati",
    "damansara", "kajang", "bangsar", "cheras", "kepong",
    "puchong", "subang", "putrajaya", "cyberjaya"
]

def load_building_keywords(filepath):
    df_building = pd.read_excel(filepath, sheet_name='building_keywords')
    keywords = set()
    for col in df_building.columns:
        col_keywords = df_building[col].dropna().astype(str).tolist()
        keywords.update([k.lower().strip() for k in col_keywords if k.strip()])
    return list(keywords)

def has_building_keyword(text, keywords):
    return any(kw in text for kw in keywords)

def clean_string(text):
    if not isinstance(text, str):
        return ""
    cleaned = (
        text.replace(",", " ").replace(".", " ").replace("_", " ")
            .replace(";", " ").replace("'", " ").replace("{", " ")
            .replace("}", " ").replace("[", " ").replace("]", " ")
            .replace("\\", " ").replace("?", " ").replace("!", " ")
            .replace("*", " ").replace(":", " ").replace("\n", " ")
            .lower()
    )
    replacements = {
        "sekolah menengah kebangsaan": "smk",
        "sekolah menengah": "smk",
        "sekolah kebangsaan": "sk",
        "sekolah rendah kebangsaan": "srk",
        "sekolah rendah": "sk",
        "sekolah jenis kebangsaan": "sjk",
        "sekolah agama": "sra",
        "kolej vokasional": "kv",
        "kolej komuniti": "kk",
        "kolej matrikulasi": "km",
        "universiti teknologi mara": "uitm",
        "universiti kebangsaan malaysia": "ukm",
        "universiti teknologi malaysia": "utm",
        "universiti sains malaysia": "usm",
        "universiti putra malaysia": "upm",
        "universiti malaysia sabah": "ums",
        "universiti malaya": "um",
        "maktab rendah sains mara": "mrsm",
        "jalan": "jln",
        "lorong": "lrg",
        "kampung": "kg",
        "taman": "tmn"
    }
    for long, abbr in replacements.items():
        cleaned = re.sub(rf"\b{re.escape(long)}\b", abbr, cleaned)
    match = re.search(r"(.*?)(\d+)$", cleaned)
    if match:
        alpha_part = match.group(1)
        numeric_part = remove_repetitive_numbers(match.group(2))
        cleaned = f"{alpha_part} {numeric_part}"
    cleaned = remove_repetitive_words(cleaned)
    return cleaned.strip()

def remove_repetitive_numbers(s):
    return re.sub(r"(\d+)\s+\1", r"\1", s)

def remove_repetitive_words(s):
    words = s.lower().split()
    seen = set()
    return " ".join([w for w in words if not (w in seen or seen.add(w))])

def detect_area_conflict(input_text, node_text):
    input_text = input_text.lower()
    node_text = node_text.lower()
    input_areas = [kw for kw in area_keywords if kw in input_text]
    node_areas = [kw for kw in area_keywords if kw in node_text]
    if input_areas and node_areas:
        return set(input_areas) != set(node_areas)
    return False

def contains_key_after_noise(cleaned_address, cleaned_key):
    try:
        pos = cleaned_address.index(cleaned_key)
        return pos > 0
    except ValueError:
        return False

def match_address_to_latlong(filepath):
    start_time = perf_counter()

    df_input = pd.read_excel(filepath, sheet_name='Input')
    df_nodes = pd.read_excel(filepath, sheet_name='Reference')
    building_keywords = load_building_keywords(filepath)

    for col in ["LL", "Matched Key", "Score"]:
        if col not in df_input.columns:
            df_input[col] = "" if col != "Score" else 0

    df_input["LL"] = df_input["LL"].astype("object")
    df_input["Matched Key"] = df_input["Matched Key"].astype("object")
    df_input["Score"] = df_input["Score"].astype("float")

    node_pool = []
    for _, row in df_nodes.iterrows():
        key = str(row.iloc[0] or "")
        ll = row.iloc[2]
        cleaned_key = clean_string(key)
        tokens = set(cleaned_key.split())
        node_pool.append({
            "key": key,
            "ll": ll,
            "cleaned_key": cleaned_key,
            "tokens": tokens
        })

    total = len(df_input)
    iterable = tqdm(df_input.iterrows(), total=total, desc="🔄 Matching") if use_tqdm else enumerate(df_input.iterrows())

    for idx, row in iterable:
        raw_address = str(row.get("full_address", ""))
        cleaned_address = clean_string(raw_address)
        tokens_input = set(cleaned_address.split())

        found = False
        for node in node_pool:
            if contains_key_after_noise(cleaned_address, node["cleaned_key"]):
                df_input.at[idx, "LL"] = node["ll"]
                df_input.at[idx, "Matched Key"] = node["key"]
                df_input.at[idx, "Score"] = 100
                found = True
                break
        if found:
            continue

        best_match = None
        for node in node_pool:
            tokens_node = node["tokens"]
            intersection = tokens_input & tokens_node
            jaccard = len(intersection) / len(tokens_node) if tokens_node else 0
            if jaccard < 0.6:
                continue

            ratio = fuzz.token_set_ratio(cleaned_address, node["cleaned_key"]) / 100
            if ratio < 0.6:
                continue

            # 📌 New scoring boosts
            score = (0.65 * jaccard + 0.35 * ratio) * 100

            if node["cleaned_key"] in cleaned_address:
                score += 10  # boost for substring presence

            if len(node["cleaned_key"].split()) >= 3 and ("pavilion" in node["cleaned_key"] or "damansara" in node["cleaned_key"]):
                score += 5  # boost for detailed or branded name

            if has_building_keyword(cleaned_address, building_keywords) and has_building_keyword(node["cleaned_key"], building_keywords):
                score += 10
            elif has_building_keyword(cleaned_address, building_keywords) != has_building_keyword(node["cleaned_key"], building_keywords):
                score -= 5

            if detect_area_conflict(cleaned_address, node["cleaned_key"]):
                score -= 5

            if not best_match or score > best_match["score"]:
                best_match = {**node, "score": round(score)}

        if best_match and best_match["score"] >= 80:
            df_input.at[idx, "LL"] = best_match["ll"]
            df_input.at[idx, "Matched Key"] = best_match["key"]
            df_input.at[idx, "Score"] = best_match["score"]
        else:
            df_input.at[idx, "LL"] = ""
            df_input.at[idx, "Matched Key"] = ""
            df_input.at[idx, "Score"] = 0

    output_path = "C:/Users/User/Desktop/Zus_match.csv"
    df_input.to_csv(output_path, index=False)

    total_time = round(perf_counter() - start_time, 2)
    print(f"\n✅ Matching complete. File saved to: {output_path}")
    print(f"⏱️ Total runtime: {timedelta(seconds=total_time)}")

# 🔽 Run
match_address_to_latlong("C:/Users/User/Desktop/Zus_Dict.xlsx")
