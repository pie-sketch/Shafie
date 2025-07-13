import pandas as pd
import re
from time import perf_counter
import spacy

start_time = perf_counter()

# --- Load input CSV ---
csv_path = r"C:\Users\User\Desktop\For kimi.csv"
try:
    df = pd.read_csv(csv_path, encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(csv_path, encoding='windows-1252')

# --- Clean non-breaking spaces ---
df.columns = df.columns.str.replace('\xa0', ' ', regex=True)
df = df.astype(str).apply(lambda col: col.str.replace('\xa0', ' ', regex=True))

# --- Load spaCy NER model ---
nlp = spacy.load("en_core_web_sm")

# --- Category rules (extended) ---
classification_rules = {
    "Shop": ["coffee", "shop", "store", "trading", "motor", "hardware", "market", "empire", "enterprise", "collection", "space", "restaurant", "tomyam", "western", "7-eleven", "99 speedmart", "watson", "seafood", "cafe", "mixue", "restoran", "ninjavan", "pasar", "nasi", "medan selera", "koperasi", "kk super mart", "giant", "food court", "econsave", "dominos", "kfc", "mcd", "pizza", "7 eleven"],
    "Education": ["school", "college", "university", "education", "preschool", "music", "tadika", "smjk", "sk", "sjk", "learning", "srjk", "universiti", "tabika", "sekolah menengah kebangsaan", "pusat pendidikan", "sekolah rendah", "sekolah", "smk", "kids", "pusat", "politeknik", "madrasah", "kolej", "kafa", "institut", "faculty", "fakulti", "akademi"],
    "Religious": ["temple", "masjid", "surau", "church", "buddhist", "kuil"],
    "Tower": ["wisma", "menara", "tower", "pejabat", "ioi", "mall", "ibu pejabat", "yayasan", "foundation", "plaza", "complex", "prima", "presint", "ppam", "pavilion", "kompleks", "jabatan", "hall", "bangunan", "balai polis", "balai bomba"],
    "Hotel": ["hotel", "resort", "inn", "rooms"],
    "Embassy": ["embassy", "consulate"],
    "Sport": ["sport", "fitness", "gym", "arena"],
    "Residence": ["residence", "residency", "homestay", "apartment", "condominium", "blok", "vista", "villa", "the", "pangsapuri", "ppr", "kuarters", "flat", "dewan", "asrama", "residensi", "perumahan awam", "condo", "parklane", "pantai hillpark", "pacific place", "kondominium", "hostel", "greenview heights", "greensville", "block", "avenue court"],
    "Medical": ["pharmacy", "pharma", "medical", "poliklinik", "farmasi", "klinik", "hospital"],
    "Street": ["lorong", "jalan", "persiaran", "lot", "gugusan"],
    "Bank": ["bank", "uob", "cimb", "hong leong"],
    "Oil Pump": ["petron", "shell", "petronas", "bhp", "caltex"],
    "Transport": ["stesen", "terminal", "perodua", "proton", "rapid rail", "mrt", "lrt", "ktm", "ktmb"],
    "Courier": ["ninjavan", "pos", "pos laju", "pos malaysia", "parcelhub", "mbe", "mail boxes", "j&t", "flash express", "dhl"]
}

# --- Extract name, type, postcode ---
def extract_name_and_type(text):
    text = str(text).lower()
    doc = nlp(text)

    # Extract name using spaCy
    name_parts = [ent.text for ent in doc.ents if ent.label_ in ("ORG", "FAC", "GPE", "LOC")]
    name = name_parts[0] if name_parts else ""

    # Classify type
    classified_type = "Unknown"
    for label, keywords in classification_rules.items():
        if any(kw in text for kw in keywords):
            classified_type = label
            break

    # Extract postcode
    postcode_match = re.search(r"\b\d{5}\b", text)
    postcode = postcode_match.group() if postcode_match else ""

    return pd.Series([name.strip().title(), classified_type, postcode])

# --- Apply extraction ---
df[['Name', 'Validation', 'Postcode Extracted']] = df['Address'].apply(extract_name_and_type)

# --- Export CSV ---
output_path = r"C:\Users\User\Desktop\For kimi - FAST OUTPUT.csv"
df.to_csv(output_path, index=False)

# --- Summary ---
total_time = perf_counter() - start_time
match_count = df["Name"].astype(bool).sum()

print(f"\n✅ Extraction complete. File saved to: {output_path}")
print(f"⏱️ Total runtime: {int(total_time // 60)} minutes {round(total_time % 60, 1)} seconds")
print(f"📌 Total matched name rows: {match_count} / {len(df)}")
