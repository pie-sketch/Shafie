import streamlit as st
import pandas as pd
from datetime import datetime

# --- Page setup ---
st.set_page_config(page_title="🕒 Task Dashboard", layout="wide")
st.title("🕒 Live Task")

# --- Hide CSV download and adjust font size ---
st.markdown("""
    <style>
        button[title="Download data as CSV"] {
            display: none !important;
        }
        .small-font .stDataFrame th,
        .small-font .stDataFrame td {
            font-size: 12px !important;
            padding: 4px 6px !important;
        }
    </style>
""", unsafe_allow_html=True)

# --- Silent auto-refresh every 120 seconds ---
st.markdown('<meta http-equiv="refresh" content="120">', unsafe_allow_html=True)

# --- View mode: Today or manual date ---
view_mode = st.radio("📅 View Mode:", ["Today Only", "Pick a Date"], horizontal=True)
if view_mode == "Today Only":
    selected_date = datetime.now().date()
else:
    selected_date = st.date_input("📆 Select Date", value=datetime.now().date())

selected_ymd = selected_date.strftime("%Y%m%d")

# --- Load data from Google Sheet ---
url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQugUlEdpHp7YR9FPHbGsVmrB1Km-VnnfsV9nnFY4Ah2Ud9kUSpxu1y663hR8ozx_bRHgGbGCoX4pZS/pub?output=csv"
df = pd.read_csv(
    url,
    header=0,
    names=["Name", "Pool Name", "Tab", "Start Time", "End Time", "Time Done", "Load", "Pool Up"],
    dtype=str,
    keep_default_na=False
)

# --- Filter to selected date only ---
df = df[df["Pool Name"].str.startswith(f"PoolMaster_{selected_ymd}", na=False)]

if df.empty:
    st.warning(f"No data found for PoolMaster_{selected_ymd}")
else:
    # --- Show 📋 Overall View for the Date ---
    st.subheader("📋 Overall View for the Date")
    day_df = df[::-1].reset_index(drop=True)
    with st.container():
        st.markdown('<div class="small-font">', unsafe_allow_html=True)
        st.dataframe(day_df, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # --- Determine latest active pool based on any timestamp ---
    df_time = df.copy()
    for col in ["Start Time", "End Time", "Pool Up"]:
        df_time[col] = pd.to_datetime(df_time[col], errors="coerce")

    df_time["latest_time"] = df_time[["Start Time", "End Time", "Pool Up"]].max(axis=1)
    df_time = df_time.dropna(subset=["latest_time"])

    if not df_time.empty:
        latest_pool = df_time.sort_values("latest_time", ascending=False)["Pool Name"].iloc[0]
    else:
        latest_pool = df["Pool Name"].dropna().unique()[-1]  # fallback

    # --- Selectbox for pools ---
    pool_names = sorted(df["Pool Name"].dropna().unique())
    default_index = pool_names.index(latest_pool) if latest_pool in pool_names else len(pool_names) - 1
    selected_pool = st.selectbox("📦 Select Pool", pool_names, index=default_index)

    # --- Show selected pool view below ---
    st.subheader(f"📊 Pool: {selected_pool}")
    pool_df = df[df["Pool Name"] == selected_pool][::-1].reset_index(drop=True)
    with st.container():
        st.markdown('<div class="small-font">', unsafe_allow_html=True)
        st.dataframe(pool_df, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

# python -m streamlit run task_dashboard.py

