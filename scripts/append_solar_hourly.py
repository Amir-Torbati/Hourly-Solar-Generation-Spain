import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import duckdb

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

TZ = ZoneInfo("Europe/Madrid")
DB_DIR = "database"
os.makedirs(DB_DIR, exist_ok=True)

today = datetime.now(TZ).date()
yesterday = today - timedelta(days=1)

def fetch_day(date_local):
    start_local = datetime.combine(date_local, datetime.min.time()).replace(tzinfo=TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc = end_local.astimezone(ZoneInfo("UTC"))

    params = {
        "start_date": start_utc.isoformat(),
        "end_date": end_utc.isoformat(),
        "time_trunc": "hour"
    }

    print(f"📡 Fetching: {date_local}")
    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json().get("indicator", {}).get("values", [])
        df = pd.DataFrame(values)

        if df.empty:
            return pd.DataFrame()

        df["timestamp_local"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
        df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")
        df["date"] = df["timestamp_local"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp_local"].dt.strftime("%H:%M")
        return df[["timestamp_local", "date", "hour", "value_mw"]]
    except Exception as e:
        print(f"❌ Error fetching {date_local}: {e}")
        return pd.DataFrame()

# --- Fetch ---
df_new = pd.concat([fetch_day(yesterday), fetch_day(today)], ignore_index=True)
if df_new.empty:
    print("⚠️ No new data to append.")
    exit()

# --- Load and merge ---
csv_path = os.path.join(DB_DIR, "solar_hourly_all.csv")
df_existing = pd.read_csv(csv_path)
df_existing["timestamp_local"] = pd.to_datetime(df_existing["timestamp_local"], utc=True).dt.tz_convert(TZ)

df_all = pd.concat([df_existing, df_new])
df_all = df_all.drop_duplicates("timestamp_local").sort_values("timestamp_local")

df_all.to_csv(os.path.join(DB_DIR, "solar_hourly_all.csv"), index=False)
df_all.to_parquet(os.path.join(DB_DIR, "solar_hourly_all.parquet"), index=False)

con = duckdb.connect(os.path.join(DB_DIR, "solar_hourly.duckdb"))
con.execute("CREATE OR REPLACE TABLE solar_hourly AS SELECT * FROM df_all")
con.close()

print(f"✅ Appended {len(df_new)} new rows. Total: {len(df_all)}")



