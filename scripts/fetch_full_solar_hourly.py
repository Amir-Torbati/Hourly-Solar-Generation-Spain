import os
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
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
OUTPUT_DIR = "database"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DATE RANGE ---
start_date = datetime(2023, 1, 1)
end_date = datetime.now()

print(f"📡 Fetching solar PV data from {start_date.date()} to {end_date.date()}...")
all_data = []
current = start_date

# --- MONTHLY LOOP ---
while current < end_date:
    next_month = current + relativedelta(months=1)
    period_end = min(next_month, end_date)

    params = {
        "start_date": current.isoformat() + "Z",
        "end_date": period_end.isoformat() + "Z",
        "time_trunc": "hour"
    }

    print(f"⏳ Fetching {current.date()} → {period_end.date()}...")

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        data = res.json()
        values = data.get("indicator", {}).get("values", [])

        df = pd.DataFrame(values)
        if not df.empty:
            df["timestamp_local"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
            df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")
            df["date"] = df["timestamp_local"].dt.date
            df["hour"] = df["timestamp_local"].dt.strftime("%H:%M")
            df_clean = df[["timestamp_local", "date", "hour", "value_mw"]]
            all_data.append(df_clean)
            print(f"✅ Collected {len(df_clean)} rows.")
        else:
            print("⚠️ No data in this period.")

    except Exception as e:
        print(f"❌ Error fetching {current.date()} - {period_end.date()}: {e}")

    current = next_month

# --- SAVE RESULTS ---
if all_data:
    df_all = pd.concat(all_data).drop_duplicates(subset=["timestamp_local"]).sort_values("timestamp_local")

    df_all.to_csv(os.path.join(OUTPUT_DIR, "solar_hourly_all.csv"), index=False)
    df_all.to_parquet(os.path.join(OUTPUT_DIR, "solar_hourly_all.parquet"), index=False)

    con = duckdb.connect(os.path.join(OUTPUT_DIR, "solar_hourly.duckdb"))
    con.execute("CREATE OR REPLACE TABLE solar_hourly AS SELECT * FROM df_all")
    con.close()

    print(f"\n✅ Saved {len(df_all)} rows to '{OUTPUT_DIR}/'")
else:
    print("⚠️ No data was collected.")
