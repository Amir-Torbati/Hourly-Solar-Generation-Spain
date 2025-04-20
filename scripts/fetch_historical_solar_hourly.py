import os
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
BASE_URL = "https://api.esios.ree.es/indicators/1293"  # Clean Solar PV
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
TZ = ZoneInfo("Europe/Madrid")
OUTPUT_DIR = "raw_solar_1293"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DATE RANGE ---
start_date_local = datetime(2023, 1, 1, tzinfo=TZ)
end_date_local = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

current = start_date_local

while current < end_date_local:
    next_day = current + relativedelta(days=1)

    start_utc = current.astimezone(ZoneInfo("UTC"))
    end_utc = next_day.astimezone(ZoneInfo("UTC"))

    params = {
        "start_date": start_utc.isoformat(),
        "end_date": end_utc.isoformat(),
        "time_trunc": "hour"
    }

    date_str = current.strftime("%Y-%m-%d")
    filename = os.path.join(OUTPUT_DIR, f"{date_str}-raw.csv")

    if os.path.exists(filename):
        print(f"✅ Already exists: {filename}")
        current = next_day
        continue

    print(f"📡 Fetching raw solar PV for {date_str}...")

    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json().get("indicator", {}).get("values", [])

        if values:
            df = pd.DataFrame(values)
            df.to_csv(filename, index=False)
            print(f"✅ Saved {len(df)} rows to {filename}")
        else:
            print(f"⚠️ No data for {date_str}")

    except Exception as e:
        print(f"❌ Error on {date_str}: {e}")

    current = next_day



