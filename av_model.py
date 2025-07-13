import pandas as pd
import re
from time import perf_counter
from datetime import timedelta
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

tqdm.pandas()

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
        "sekolah menengah kebangsaan": "smk", "sekolah menengah": "smk",
        "sekolah kebangsaan": "sk", "sekolah rendah kebangsaan": "srk",
        "sekolah rendah": "sk", "sekolah jenis kebangsaan": "sjk",
        "sekolah agama": "sra", "kolej vokasional": "kv",
        "kolej komuniti": "kk", "kolej matrikulasi": "km",
        "universiti teknologi mara": "uitm", "universiti kebangsaan malaysia": "ukm",
        "universiti teknologi malaysia": "utm", "universiti sains malaysia": "usm",
        "universiti putra malaysia": "upm", "universiti malaysia sabah": "ums",
        "universiti malaya": "um", "maktab rendah sains mara": "mrsm",
        "jalan": "jln", "lorong": "lrg", "kampung": "kg", "taman": "tmn"
    }
    for long, abbr in replacements.items():
        cleaned = re.sub(rf"\b{re.escape(long)}\b", abbr, cleaned)

    match = re.search(r"(.*?)(\d+)$", cleaned)
    if match:
        alpha_part = match.group(1)
        numeric_part = re.sub(r"(\d+)\s+\1", r"\1", match.group(2))
        cleaned = f"{alpha_part} {numeric_part}"

    words = cleaned.split()
    seen = set()
    return " ".join([w for w in words if not (w in seen or seen.add(w))]).strip()

def match_address_to_latlong(filepath):
    start_time = perf_counter()
    df_input = pd.read_excel(filepath, sheet_name='Input')
    df_nodes = pd.read_excel(filepath, sheet_name='Reference')

    for col in ["LL", "Matched Key", "Score"]:
        if col not in df_input.columns:
            df_input[col] = "" if col != "Score" else 0

    df_input["LL"] = df_input["LL"].astype("object")
    df_input["Matched Key"] = df_input["Matched Key"].astype("object")
    df_input["Score"] = df_input["Score"].astype("float")

    # Preprocess reference list
    node_pool = []
    for _, row in df_nodes.iterrows():
        key = str(row.iloc[0] or "")
        ll = row.iloc[2]
        cleaned_key = clean_string(key)
        node_pool.append({"key": key, "ll": ll, "cleaned_key": cleaned_key})

    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 2))
    ref_matrix = vectorizer.fit_transform([n['cleaned_key'] for n in node_pool])

    # Modern progress bar using tqdm
    for idx in tqdm(range(len(df_input)), desc="🔍 Matching", unit="row"):
        row = df_input.iloc[idx]
        raw_address = str(row.get("full_address", ""))
        postcode = str(row.get("postcode", ""))
        cleaned_address = clean_string(raw_address)

        input_vec = vectorizer.transform([cleaned_address])
        sim_scores = cosine_similarity(input_vec, ref_matrix).flatten()

        best_idx = sim_scores.argmax()
        best_score = sim_scores[best_idx]
        final_score = round(best_score * 100)

        if final_score >= 80:
            if postcode == "53300" and final_score < 85:
                pass
            else:
                best_node = node_pool[best_idx]
                df_input.at[idx, "LL"] = best_node["ll"]
                df_input.at[idx, "Matched Key"] = best_node["key"]
                df_input.at[idx, "Score"] = final_score
                continue

        df_input.at[idx, "LL"] = ""
        df_input.at[idx, "Matched Key"] = ""
        df_input.at[idx, "Score"] = 0

    df_input.to_csv("C:/Users/User/Desktop/model_match.csv", index=False)
    total_time = round(perf_counter() - start_time, 2)
    print(f"\n✅ Matching complete. File saved to: C:/Users/User/Desktop/model_match.csv")
    print(f"⏱️ Total runtime: {timedelta(seconds=int(total_time))}")

# 🔽 Run it
match_address_to_latlong("C:/Users/User/Desktop/av_model.xlsx")
