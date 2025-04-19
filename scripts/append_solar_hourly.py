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

def extract_date(path):
    return os.path.basename(path).split("-hourly.csv")[0]

# --- Load new raw data ---
all_paths = sorted(glob(os.path.join(DATA_DIR, "*-hourly.csv")))
all_new = []

for path in all_paths:
    try:
        df = pd.read_csv(path)
        if df.empty or "datetime" not in df.columns:
            continue

        df["timestamp_local"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
        df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")

        file_date = extract_date(path)
        start = pd.Timestamp(file_date, tz=TZ)
        end = start + pd.Timedelta(days=1)
        df = df[(df["timestamp_local"] >= start) & (df["timestamp_local"] < end)]

        df["date"] = df["timestamp_local"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp_local"].dt.strftime("%H:%M")
        df_clean = df[["timestamp_local", "date", "hour", "value_mw"]]
        all_new.append(df_clean)

    except Exception as e:
        print(f"❌ Error loading {path}: {e}")

# --- Load existing from CSV (master) ---
csv_path = os.path.join(DB_DIR, "solar_hourly_all.csv")
if os.path.exists(csv_path):
    df_existing = pd.read_csv(csv_path)
    df_existing["timestamp_local"] = pd.to_datetime(df_existing["timestamp_local"], utc=True).dt.tz_convert(TZ)
else:
    df_existing = pd.DataFrame(columns=["timestamp_local", "date", "hour", "value_mw"])

# --- Merge and deduplicate ---
df_new = pd.concat(all_new, ignore_index=True) if all_new else pd.DataFrame(columns=df_existing.columns)
df_all = pd.concat([df_existing, df_new], ignore_index=True)
df_all = df_all.drop_duplicates(subset=["timestamp_local"]).sort_values("timestamp_local")

# --- Format timestamp_local to fixed string format (for CSV/parquet)
df_all["timestamp_local"] = pd.to_datetime(df_all["timestamp_local"]).dt.strftime("%Y-%m-%d %H:%M:%S%z")

# --- Save all formats ---
df_all.to_csv(os.path.join(DB_DIR, "solar_hourly_all.csv"), index=False)
df_all.to_parquet(os.path.join(DB_DIR, "solar_hourly_all.parquet"), index=False)

# --- Save to DuckDB ---
con = duckdb.connect(os.path.join(DB_DIR, "solar_hourly.duckdb"))
con.execute("CREATE OR REPLACE TABLE solar_hourly AS SELECT * FROM df_all")
con.close()

print(f"✅ Saved all formats: CSV, Parquet, DuckDB — {len(df_all)} total rows.")






