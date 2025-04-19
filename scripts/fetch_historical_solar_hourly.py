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

start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

all_data = []
current = start_date_local

while current < end_date_local:
    next_month = current + relativedelta(months=1)
    end_local = min(next_month, end_date_local)

    start_utc = current.astimezone(ZoneInfo("UTC"))
    end_utc = end_local.astimezone(ZoneInfo("UTC"))

    params = {
        "start_date": start_utc.isoformat(),
        "end_date": end_utc.isoformat(),
        "time_trunc": "hour"
    }

    print(f"⏳ Fetching {current.date()} to {end_local.date()}...")

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json().get("indicator", {}).get("values", [])
        df = pd.DataFrame(values)

        if not df.empty:
            df["timestamp_local"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(TZ)
            df["value_mw"] = pd.to_numeric(df["value"], errors="coerce")
            df["date"] = df["timestamp_local"].dt.strftime("%Y-%m-%d")
            df["hour"] = df["timestamp_local"].dt.strftime("%H:%M")
            df_clean = df[["timestamp_local", "date", "hour", "value_mw"]]
            all_data.append(df_clean)
        else:
            print("⚠️ No data for this month.")

    except Exception as e:
        print(f"❌ Error on {current.date()}: {e}")

    current = end_local

if all_data:
    df_all = pd.concat(all_data).drop_duplicates("timestamp_local").sort_values("timestamp_local")

    df_all.to_csv(os.path.join(OUTPUT_DIR, "solar_hourly_all.csv"), index=False)
    df_all.to_parquet(os.path.join(OUTPUT_DIR, "solar_hourly_all.parquet"), index=False)

    con = duckdb.connect(os.path.join(OUTPUT_DIR, "solar_hourly.duckdb"))
    con.execute("CREATE OR REPLACE TABLE solar_hourly AS SELECT * FROM df_all")
    con.close()

    print(f"✅ Saved {len(df_all)} rows to {OUTPUT_DIR}/")
else:
    print("⚠️ No data collected.")

