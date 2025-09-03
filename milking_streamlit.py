####
import os
import sqlite3
import hashlib
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="🐄 Milk Yield Dashboard",
    page_icon="🥛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 로고 표시
logo_path = os.path.join(os.path.dirname(__file__), "cj.jpg")
if os.path.exists(logo_path):
    st.image(logo_path, width=200)

DB_PATH = os.path.join(os.path.dirname(__file__), "animals.db")

# ──────────────────────────────────────────────────────────────────────────────
# Database connection (cached so it isn’t pickled)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

conn = get_db_conn()

# ──────────────────────────────────────────────────────────────────────────────
# Farm-name anonymization (NO literal farm names in code)
# ──────────────────────────────────────────────────────────────────────────────
# 사전 등록된 보호 대상 농장명(소문자)을 SHA-256 해시로만 보관하여 익명화 매핑
# "magpantay".lower() → sha256: 82b7a67e7bd24e71bbaf80e360039b7fed7805b11a1b6f35c3926c9c9812c9fd
# "samabaco".lower()  → sha256: 687d0aa07ed9519f1a55d848b562e29044a742a7d6daa44a273735ccb4fb8591
PROTECTED_HASH_TO_ALIAS = {
    "82b7a67e7bd24e71bbaf80e360039b7fed7805b11a1b6f35c3926c9c9812c9fd": "K-farm",
    "687d0aa07ed9519f1a55d848b562e29044a742a7d6daa44a273735ccb4fb8591": "J-farm",
}

def _sha256_lower(s: str) -> str:
    return hashlib.sha256((s or "").lower().encode("utf-8")).hexdigest()

def anonymize_farm_name(actual_name: str) -> str:
    """DB에서 읽은 실제 농장명을 시연용 별칭으로 변환."""
    h = _sha256_lower(actual_name or "")
    return PROTECTED_HASH_TO_ALIAS.get(h, actual_name or "")

@st.cache_data
def build_farm_alias_maps():
    """
    DB의 실제 farm_name 목록을 읽어, 화면표시용 alias와 역매핑을 구성.
    - alias_to_internal: 화면에서 선택된 alias → 실제 farm_name
    - internal_to_alias: 실제 farm_name → 화면표시 alias
    """
    df = pd.read_sql(
        "SELECT DISTINCT farm_name FROM animals WHERE COALESCE(farm_name,'')<>'' ORDER BY farm_name",
        conn
    )
    alias_to_internal = {}
    internal_to_alias = {}

    for actual in df["farm_name"].tolist():
        alias = anonymize_farm_name(actual)

        # 혹시 alias 충돌 시 숫자 suffix
        base = alias if alias else ""
        i = 2
        while alias in alias_to_internal and alias_to_internal[alias] != actual:
            alias = f"{base}-{i}"
            i += 1

        alias_to_internal[alias] = actual
        internal_to_alias[actual] = alias

    aliases_sorted = sorted(alias_to_internal.keys(), key=lambda x: x.lower())
    return aliases_sorted, alias_to_internal, internal_to_alias

# ──────────────────────────────────────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load_animals(internal_farm_name: str):
    return pd.read_sql(
        """
        SELECT ear_tag, birth_date
          FROM animals
         WHERE farm_name = ?
         ORDER BY ear_tag
        """,
        conn,
        params=(internal_farm_name,),
    )

@st.cache_data
def farm_yield_summary(internal_farm_name: str):
    total = pd.read_sql(
        "SELECT COALESCE(SUM(m.yield_value),0) AS total_liters "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=?",
        conn, params=(internal_farm_name,)
    )["total_liters"].iloc[0]

    days = pd.read_sql(
        "SELECT COUNT(DISTINCT record_date) AS lactation_days "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=?",
        conn, params=(internal_farm_name,)
    )["lactation_days"].iloc[0]

    df_year = pd.read_sql(
        "SELECT record_year AS year, "
        "       SUM(m.yield_value)            AS liters, "
        "       COUNT(DISTINCT m.record_date) AS days, "
        "       COUNT(DISTINCT m.ear_tag)     AS cows "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=? "
        "GROUP BY record_year ORDER BY record_year",
        conn, params=(internal_farm_name,)
    )
    if not df_year.empty:
        df_year["avg_daily"] = (df_year["liters"] / df_year["days"]).round(1)
    else:
        df_year["avg_daily"] = pd.Series(dtype=float)

    since = (date.today() - timedelta(days=365)).isoformat()
    df_12mo = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       SUM(yield_value)               AS liters "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(internal_farm_name, since)
    )
    if not df_12mo.empty:
        df_12mo["liters"] = df_12mo["liters"].round(0).astype(int)

    df_count = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       COUNT(DISTINCT m.ear_tag)     AS sessions "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(internal_farm_name, since)
    )

    if not df_12mo.empty:
        df_12mo = df_12mo.merge(df_count, on="month", how="left").fillna(0)
        df_12mo["sessions"] = df_12mo["sessions"].astype(int)

    return total, days, df_year, df_12mo

@st.cache_data
def animal_yield_summary(ear_tag: str):
    total = pd.read_sql(
        "SELECT COALESCE(SUM(yield_value),0) AS total_liters "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    )["total_liters"].iloc[0]

    days = pd.read_sql(
        "SELECT COUNT(DISTINCT record_date) AS lactation_days "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    )["lactation_days"].iloc[0]

    rng = pd.read_sql(
        "SELECT MIN(record_date) AS mn, MAX(record_date) AS mx "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    ).iloc[0]
    try:
        d0 = pd.to_datetime(rng["mn"]).date()
        d1 = pd.to_datetime(rng["mx"]).date()
        duration = (d1 - d0).days + 1
    except Exception:
        duration = days

    df_year = pd.read_sql(
        "SELECT record_year AS year, "
        "       SUM(yield_value)             AS liters, "
        "       COUNT(DISTINCT record_date)   AS days "
        "FROM milk_yield WHERE ear_tag=? "
        "GROUP BY record_year ORDER BY record_year",
        conn, params=(ear_tag,)
    )
    if not df_year.empty:
        df_year["avg_daily"] = (df_year["liters"] / df_year["days"]).round(1)
    else:
        df_year["avg_daily"] = pd.Series(dtype=float)

    since = (date.today() - timedelta(days=365)).isoformat()
    df_12mo = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       SUM(yield_value)               AS liters "
        "FROM milk_yield WHERE ear_tag=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(ear_tag, since)
    )
    if not df_12mo.empty:
        df_12mo["liters"] = df_12mo["liters"].round(0).astype(int)

    df_sessions = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       COUNT(*)                      AS sessions "
        "FROM milk_yield WHERE ear_tag=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(ear_tag, since)
    )
    if not df_12mo.empty:
        df_12mo = df_12mo.merge(df_sessions, on="month", how="left").fillna(0)
        df_12mo["sessions"] = df_12mo["sessions"].astype(int)

    stats = pd.read_sql(
        "SELECT ROUND(AVG(yield_value),1) AS avg_daily, "
        "       MAX(yield_value)            AS max_yield, "
        "       MIN(yield_value)            AS min_yield "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    ).iloc[0].to_dict()

    return total, days, duration, df_year, df_12mo, stats

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar (farms shown as aliases; internal names hidden)
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.title("Configuration")

aliases, alias_to_internal, internal_to_alias = build_farm_alias_maps()
farm_alias = st.sidebar.selectbox("🐄 Select Farm", ["-- pick a farm --"] + aliases)
mode = st.sidebar.radio("🔍 View Mode", ["Farm Overview", "Individual Cow"])

if farm_alias == "-- pick a farm --":
    st.sidebar.warning("Please select a farm to proceed.")
    st.stop()

# 선택된 표시용 별칭 → 실제 DB 농장명으로 변환(쿼리는 내부명 사용)
farm_internal = alias_to_internal.get(farm_alias, "")

# ──────────────────────────────────────────────────────────────────────────────
# Main content
# ──────────────────────────────────────────────────────────────────────────────
if mode == "Farm Overview":
    # 화면 표시는 alias, 쿼리는 internal
    st.header(f"📋 Farm Overview: **{farm_alias}**")
    total, days, df_year, df_12mo = farm_yield_summary(farm_internal)

    c1, c2, c3 = st.columns([1,1,2])
    c1.metric("Total Milk (L)", f"{total:,.0f}")
    c2.metric("Total Lactation Days", f"{days}")
    c3.metric("Cows Registered", len(load_animals(farm_internal)))

    with st.expander("📊 Annual Production Breakdown"):
        if not df_year.empty:
            df_disp = df_year.rename(columns={
                "year":"Year",
                "liters":"Total Liters",
                "days":"Days Milked",
                "avg_daily":"Avg Daily (L)"
            }).set_index("Year")
            df_disp["Total Liters"]  = df_disp["Total Liters"].map("{:,.0f}".format)
            df_disp["Days Milked"]   = df_disp["Days Milked"].map("{:,.0f}".format)
            df_disp["Avg Daily (L)"] = df_disp["Avg Daily (L)"].map("{:,.1f}".format)
            st.dataframe(df_disp)
        else:
            st.info("No annual data.")

    with st.expander("📈 Last 12 Months Trend"):
        if not df_12mo.empty:
            fig, ax1 = plt.subplots(figsize=(10,4))
            ax1.bar(df_12mo["month"], df_12mo["liters"], color="tab:blue", alpha=0.6)
            ax1.set_ylabel("Liters")
            ax1.set_xticks(range(len(df_12mo)))
            ax1.set_xticklabels(df_12mo["month"], rotation=45)

            ax2 = ax1.twinx()
            ax2.plot(df_12mo["month"], df_12mo["sessions"],
                     marker="o", color="tab:orange", linewidth=2)
            ax2.set_ylabel("Milking Sessions")

            plt.title("Last 12 Months: Production & Sessions")
            plt.tight_layout()
            st.pyplot(fig)

            tbl = df_12mo.rename(columns={
                "month":"Month",
                "liters":"Liters",
                "sessions":"Milking Sessions"
            }).set_index("Month")
            tbl["Liters"]           = tbl["Liters"].map("{:,.0f}".format)
            tbl["Milking Sessions"] = tbl["Milking Sessions"].map("{:,.0f}".format)
            st.table(tbl)
        else:
            st.info("No data for the past 12 months.")

    # CSV 다운로드 (파일명에도 내부 농장명 대신 alias 사용)
    csv = df_year.copy()
    if not csv.empty:
        csv["Avg Daily (L)"] = csv["avg_daily"]
        csv = csv[["year","liters","days","avg_daily"]]
        csv.columns = ["Year","Total Liters","Days Milked","Avg Daily (L)"]
        csv["Total Liters"]   = csv["Total Liters"].map("{:,.0f}".format)
        csv["Days Milked"]    = csv["Days Milked"].map("{:,.0f}".format)
        csv["Avg Daily (L)"]  = csv["Avg Daily (L)"].map("{:,.1f}".format)
    st.download_button(
        "⬇️ Download Farm Annual CSV",
        (csv.to_csv(index=False).encode("utf-8") if not csv.empty else "".encode("utf-8")),
        file_name=f"{farm_alias}_annual_report.csv",
        mime="text/csv"
    )

else:
    df_animals = load_animals(farm_internal)
    cow = st.sidebar.selectbox(
        "🐮 Select Cow", ["-- pick a cow --"] + df_animals["ear_tag"].tolist()
    )
    if cow == "-- pick a cow --":
        st.sidebar.warning("Please select a cow.")
        st.stop()

    st.header(f"🐮 Cow Detail: **{cow}**")
    birth = df_animals.set_index("ear_tag").at[cow,"birth_date"]
    age   = ((date.today() - pd.to_datetime(birth).date()).days // 30) if birth else "-"
    col1, col2 = st.columns(2)
    col1.metric("Birth Date", birth or "-")
    col2.metric("Age (months)", f"{age}")

    total, days, duration, df_year, df_12mo, stats = animal_yield_summary(cow)
    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Total Milk (L)", f"{total:,.0f} ({duration} d)")
    m2.metric("Days Milked", f"{days}")
    m3.metric("Avg Daily (L)", f"{stats.get('avg_daily', 0):.1f}")
    m4.metric("Max / Min (L)", f"{stats.get('max_yield','-')} / {stats.get('min_yield','-')}")

    tab1, tab2 = st.tabs(["Yearly Trend","Last 12 Months"])
    with tab1:
        if not df_year.empty:
            df_disp = df_year.rename(columns={
                "year":"Year",
                "liters":"Total Liters",
                "days":"Days Milked",
                "avg_daily":"Avg Daily (L)"
            }).set_index("Year")
            df_disp["Total Liters"]  = df_disp["Total Liters"].map("{:,.0f}".format)
            df_disp["Days Milked"]   = df_disp["Days Milked"].map("{:,.0f}".format)
            df_disp["Avg Daily (L)"] = df_disp["Avg Daily (L)"].map("{:,.1f}".format)
            st.dataframe(df_disp)
        else:
            st.info("No yearly data.")

    with tab2:
        if not df_12mo.empty:
            fig, ax1 = plt.subplots(figsize=(10,4))
            ax1.bar(df_12mo["month"], df_12mo["liters"], color="tab:blue", alpha=0.6)
            ax1.set_ylabel("Liters")
            ax1.set_xticks(range(len(df_12mo)))
            ax1.set_xticklabels(df_12mo["month"], rotation=45)

            ax2 = ax1.twinx()
            ax2.plot(df_12mo["month"], df_12mo["sessions"],
                     marker="o", color="tab:orange", linewidth=2)
            ax2.set_ylabel("Milking Sessions")

            plt.title("Last 12 Months: Production & Sessions")
            plt.tight_layout()
            st.pyplot(fig)

            tbl = df_12mo.rename(columns={
                "month":"Month",
                "liters":"Liters",
                "sessions":"Milking Sessions"
            }).set_index("Month")
            tbl["Liters"]           = tbl["Liters"].map("{:,.0f}".format)
            tbl["Milking Sessions"] = tbl["Milking Sessions"].map("{:,.0f}".format)
            st.table(tbl)
        else:
            st.info("No data for the past 12 months.")

    st.subheader("📋 Last 6 Months Detail")
    df_6mo = pd.read_sql(
        "SELECT record_date AS Date, yield_value AS Liters "
        "FROM milk_yield WHERE ear_tag=? AND record_date>=? ORDER BY record_date",
        conn, params=(cow,(date.today()-timedelta(days=180)).isoformat())
    )
    if not df_6mo.empty:
        df_6mo["Liters"] = df_6mo["Liters"].map("{:,.0f}".format)
        st.dataframe(df_6mo.set_index("Date"))
    else:
        st.info("No data in the last six months.")

    # CSV 다운로드 (파일명에 alias 사용)
    csv = df_year.copy()
    if not csv.empty:
        csv["Avg Daily (L)"] = csv["avg_daily"]
        csv = csv[["year","liters","days","avg_daily"]]
        csv.columns = ["Year","Total Liters","Days Milked","Avg Daily (L)"]
        csv["Total Liters"]   = csv["Total Liters"].map("{:,.0f}".format)
        csv["Days Milked"]    = csv["Days Milked"].map("{:,.0f}".format)
        csv["Avg Daily (L)"]  = csv["Avg Daily (L)"].map("{:,.1f}".format)
    st.download_button(
        "⬇️ Download Cow Annual CSV",
        (csv.to_csv(index=False).encode("utf-8") if not csv.empty else "".encode("utf-8")),
        file_name=f"{farm_alias}_{cow}_annual_report.csv",
        mime="text/csv"
    )
