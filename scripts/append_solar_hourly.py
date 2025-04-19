import os
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import duckdb

# --- CONFIG ---
DATA_DIR = "data"
DB_DIR = "database"
TZ = ZoneInfo("Europe/Madrid")
TODAY = datetime.now(TZ).date()
YESTERDAY = TODAY - timedelta(days=1)

# --- Load daily file safely ---
def load_daily_file(date):
    filename = f"{date}-hourly.csv"
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"⚠️ Missing file: {path}")
        return pd.DataFrame()
    
    df = pd.read_csv(path)
    df["timestamp_local"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Europe/Madrid")
    df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = df["timestamp_local"].dt.date
    df["hour"] = df["timestamp_local"].dt.strftime("%H:%M")
    return df[["timestamp_local", "date", "hour", "value_mw"]]

# --- Combine yesterday + today ---
df_new = pd.concat([
    load_daily_file(YESTERDAY),
    load_daily_file(TODAY)
], ignore_index=True)

if df_new.empty:
    print("⚠️ No new data to append.")
    exit()

# --- Load existing main database ---
db_csv = os.path.join(DB_DIR, "solar_hourly_all.csv")
df_existing = pd.read_csv(db_csv)

# 🛠️ FIX: Parse timestamp_local correctly!
df_existing["timestamp_local"] = pd.to_datetime(df_existing["timestamp_local"]).dt.tz_convert("Europe/Madrid")

# --- Merge + Deduplicate ---
df_all = pd.concat([df_existing, df_new])
df_all = df_all.drop_duplicates(subset=["timestamp_local"]).sort_values("timestamp_local")

# --- Save all formats ---
os.makedirs(DB_DIR, exist_ok=True)
df_all.to_csv(os.path.join(DB_DIR, "solar_hourly_all.csv"), index=False)
df_all.to_parquet(os.path.join(DB_DIR, "solar_hourly_all.parquet"), index=False)

con = duckdb.connect(os.path.join(DB_DIR, "solar_hourly.duckdb"))
con.execute("CREATE OR REPLACE TABLE solar_hourly AS SELECT * FROM df_all")
con.close()

print(f"✅ Appended and saved updated solar DB ({len(df_all)} rows).")

