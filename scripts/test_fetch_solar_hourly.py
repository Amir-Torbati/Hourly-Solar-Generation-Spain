# scripts/test_fetch_solar_hourly.py

import os
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import duckdb

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
OUTPUT_DIR = "test_database"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DATE RANGE ---
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 1, 1)  # exclusive
print(f"📡 Fetching solar PV data for 2023...")

all_data = []

# --- MONTHLY FETCH LOOP ---
current = start_date
while current < end_date:
    next_month = current + relativedelta(months=1)
    period_end = min(next_month, end_date)

    params = {
        "start_date": current.isoformat() + "Z",
        "end_date": period_end.isoformat() + "Z",
        "time_trunc": "hour"
    }

    print(f"⏳ Fetching {current.date()} → {period_end.date()}")

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        data = res.json()
        values = data.get("indicator", {}).get("values", [])

        df = pd.DataFrame(values)
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            all_data.append(df)
            print(f"✅ {len(df)} rows")
        else:
            print("⚠️ No data in this period.")

    except Exception as e:
        print(f"❌ Error on {current.date()}: {e}")

    current = period_end

# --- SAVE ---
if all_data:
    df_all = pd.concat(all_data, ignore_index=True)
    csv_path = f"{OUTPUT_DIR}/solar_raw_2023.csv"
    parquet_path = f"{OUTPUT_DIR}/solar_raw_2023.parquet"
    duckdb_path = f"{OUTPUT_DIR}/solar_raw_2023.duckdb"

    df_all.to_csv(csv_path, index=False)
    df_all.to_parquet(parquet_path, index=False)

    con = duckdb.connect(duckdb_path)
    con.execute("CREATE OR REPLACE TABLE solar_raw AS SELECT * FROM df_all")
    con.close()

    print(f"\n✅ Saved {len(df_all)} total rows to '{OUTPUT_DIR}/'")
    print(f"📁 Files: {os.listdir(OUTPUT_DIR)}")
else:
    print("⚠️ No data fetched.")
