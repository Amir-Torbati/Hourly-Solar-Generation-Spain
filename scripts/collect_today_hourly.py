import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os

API_TOKEN = "YOUR_API_TOKEN"  # Replace with your real one
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

now_local = datetime.now(ZoneInfo("Europe/Madrid")).replace(second=0, microsecond=0)
start_local = now_local.replace(hour=0, minute=0)

now_utc = now_local.astimezone(timezone.utc)
start_utc = start_local.astimezone(timezone.utc)

today_str = start_local.strftime("%Y-%m-%d")
file_path = f"data/{today_str}-hourly.csv"
os.makedirs("data", exist_ok=True)

df_existing = pd.DataFrame()
if os.path.exists(file_path):
    df_existing = pd.read_csv(file_path, parse_dates=["datetime"])

params = {
    "start_date": start_utc.isoformat(),
    "end_date": now_utc.isoformat(),
    "time_trunc": "hour"
}

print(f"📡 Fetching hourly data from {start_local} to {now_local}...")

res = requests.get(BASE_URL, headers=HEADERS, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])
df_new = df_new.sort_values("datetime")

df_combined = pd.concat([df_existing, df_new])
df_combined = df_combined.drop_duplicates(subset=["datetime"]).sort_values("datetime")

df_combined.to_csv(file_path, index=False)
print(f"✅ Saved {len(df_combined)} rows to {file_path}")
