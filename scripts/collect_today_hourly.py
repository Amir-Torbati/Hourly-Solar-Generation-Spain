import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os

# --- Config ---
API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

# --- Spain local time range (midnight to now) ---
now_local = datetime.now(ZoneInfo("Europe/Madrid")).replace(second=0, microsecond=0)
start_local = now_local.replace(hour=0, minute=0)

now_utc = now_local.astimezone(timezone.utc)
start_utc = start_local.astimezone(timezone.utc)

# --- Output file ---
today_str = start_local.strftime("%Y-%m-%d")
daily_file = f"data/{today_str}-hourly.csv"
os.makedirs("data", exist_ok=True)

# --- Load existing data if any ---
df_existing = pd.DataFrame()
if os.path.exists(daily_file):
    df_existing = pd.read_csv(daily_file, parse_dates=["datetime"])

# --- API Request ---
params = {
    "start_date": start_utc.isoformat(),
    "end_date": now_utc.isoformat(),
    "time_trunc": "hour"
}

print(f"📡 Fetching hourly data from {start_local} to {now_local}...")

res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

# --- Process new data ---
df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])
df_new = df_new.sort_values("datetime")

# --- Merge and deduplicate ---
df_combined = pd.concat([df_existing, df_new])
df_combined = df_combined.drop_duplicates(subset=["datetime"]).sort_values("datetime")

# --- Save ---
df_combined.to_csv(daily_file, index=False)

print(f"✅ Synced {len(df_combined)} hourly rows to {daily_file}")
