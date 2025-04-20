import os
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/1293"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
TZ = ZoneInfo("Europe/Madrid")
OUTPUT_DIR = "database"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DATE RANGE ---
start_date_local = datetime(2023, 1, 1, 0, 0, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

# --- COLLECTING IN MONTHLY CHUNKS ---
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
            print("⚠️ No data returned")

    except Exception as e:
        print(f"❌ Error on {current.date()}: {e}")

    current = end_local

# --- SAVE RAW CSV ---
if all_data:
    df_all = pd.concat(all_data, ignore_index=True)
    output_path = os.path.join(OUTPUT_DIR, "solar_raw_1293.csv")
    df_all.to_csv(output_path, index=False)
    print(f"✅ Saved {len(df_all)} rows to {output_path}")
else:
    print("⚠️ No data collected.")




