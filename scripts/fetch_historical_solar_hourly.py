import os
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/2526"  # ✅ Real PV indicator
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
TZ = ZoneInfo("Europe/Madrid")
OUTPUT_DIR = "main_database"  # ✅ New folder
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DATE RANGE ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

# --- FETCH LOOP ---
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

    print(f"📡 Fetching {current.date()} → {end_local.date()}")

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json().get("indicator", {}).get("values", [])
        if values:
            df = pd.DataFrame(values)
            all_data.append(df)
            print(f"✅ Got {len(df)} rows")
        else:
            print("⚠️ No data for this month.")

    except Exception as e:
        print(f"❌ Error on {current.date()}: {e}")

    current = end_local

# --- SAVE RAW DATA ---
if all_data:
    df_all = pd.concat(all_data, ignore_index=True)
    out_csv = os.path.join(OUTPUT_DIR, "solar_raw_2526.csv")
    df_all.to_csv(out_csv, index=False)
    print("📁 File saved to:", out_csv)
    print(df_all.head(2))
else:
    print("⚠️ No data collected.")







