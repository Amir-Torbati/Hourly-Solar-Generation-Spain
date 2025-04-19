import os
import pandas as pd
from glob import glob
from datetime import datetime
from zoneinfo import ZoneInfo
import duckdb

# --- CONFIG ---
DATA_DIR = "data"
DB_DIR = "database"
TZ = ZoneInfo("Europe/Madrid")
os.makedirs(DB_DIR, exist_ok=True)

# --- Utility: Extract date from filename ---
def extract_date_from_filename(path):
    # Expects filename like data/2025-04-20-hourly.csv
    base = os.path.basename(path)
    return base.split("-hourly.csv")[0]

# --- Gather all daily raw CSVs ---
all_paths = sorted(glob(os.path.join(DATA_DIR, "*-hourly.csv")))
print(f"🔍 Found {len(all_paths)} raw CSVs...")

all_data = []

for path in all_paths:
    try:
        df = pd.read_csv(path)
        if df.empty or "datetime" not in df.columns:
            print(f"⚠️ Skipping invalid or empty file: {path}")
            continue

        # Convert datetime to local tz-aware
        df["timestamp_local"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
        df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")

        # Filter to only include correct local calendar day
        file_date = extract_date_from_filename(path)
        start = pd.Timestamp(file_date, tz=TZ)
        end = start + pd.Timedelta(days=1)
        df = df[(df["timestamp_local"] >= start) & (df["timestamp_local"] < end)]

        df["date"] = df["timestamp_local"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp_local"].dt.strftime("%H:%M")
        df_clean = df[["timestamp_local", "date", "hour", "value_mw"]]

        all_data.append(df_clean)

    except Exception as e:
        print(f"❌ Error in file {path}: {e}")

# --- Merge, deduplicate, sort ---
if all_data:
    df_all = pd.concat(all_data, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["timestamp_local"]).sort_values("timestamp_local")

    # Save in all formats
    df_all.to_csv(os.path.join(DB_DIR, "solar_hourly_all.csv"), index=False)
    df_all.to_parquet(os.path.join(DB_DIR, "solar_hourly_all.parquet"), index=False)

    con = duckdb.connect(os.path.join(DB_DIR, "solar_hourly.duckdb"))
    con.execute("CREATE OR REPLACE TABLE solar_hourly AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Saved {len(df_all)} tidy rows to {DB_DIR}/")
else:
    print("⚠️ No tidy data created.")




