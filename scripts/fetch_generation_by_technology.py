import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import duckdb

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/1813"  # Generation by technology
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
TZ = ZoneInfo("Europe/Madrid")
OUTPUT_DIR = "database"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- TIME RANGE ---
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
start_date_local = end_date_local - timedelta(days=1)

start_utc = start_date_local.astimezone(timezone.utc)
end_utc = end_date_local.astimezone(timezone.utc)

params = {
    "start_date": start_utc.isoformat(),
    "end_date": end_utc.isoformat(),
    "time_trunc": "hour"
}

print(f"📡 Fetching from {start_date_local} → {end_date_local}...")

try:
    res = requests.get(BASE_URL, headers=HEADERS, params=params)
    res.raise_for_status()
    values = res.json().get("indicator", {}).get("values", [])

    df = pd.DataFrame(values)
    df["timestamp_local"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
    df["technology"] = df["technology"]
    df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")

    df_clean = df[["timestamp_local", "technology", "value_mw"]].dropna()
    df_clean = df_clean.sort_values(["timestamp_local", "technology"])

    # Save all formats
    df_clean.to_csv(f"{OUTPUT_DIR}/generation_by_technology.csv", index=False)
    df_clean.to_parquet(f"{OUTPUT_DIR}/generation_by_technology.parquet", index=False)

    con = duckdb.connect(f"{OUTPUT_DIR}/generation_by_technology.duckdb")
    con.execute("CREATE OR REPLACE TABLE generation_by_technology AS SELECT * FROM df_clean")
    con.close()

    print(f"✅ Saved {len(df_clean)} rows to: {OUTPUT_DIR}/")
except Exception as e:
    print("❌ Error:", e)
