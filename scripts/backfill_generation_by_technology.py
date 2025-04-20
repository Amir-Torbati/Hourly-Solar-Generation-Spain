import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil.rrule import rrule, DAILY
import time

# --- CONFIG ---
API_TOKEN = os.environ["ESIOS_API_TOKEN"]
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}
BASE_URL = "https://api.esios.ree.es/indicators/1813"  # Generation by tech
TZ = ZoneInfo("Europe/Madrid")
OUTDIR = "test_tech_raw"
os.makedirs(OUTDIR, exist_ok=True)

# --- DATE RANGE ---
start_date = datetime(2023, 1, 1, tzinfo=TZ)
end_date = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

for dt_local in rrule(DAILY, dtstart=start_date, until=end_date):
    filename = os.path.join(OUTDIR, f"{dt_local.date()}-by-tech.csv")
    if os.path.exists(filename):
        print(f"✅ Already exists: {filename}")
        continue

    dt_utc_start = dt_local.astimezone(timezone.utc)
    dt_utc_end = (dt_local + timedelta(days=1)).astimezone(timezone.utc)

    params = {
        "start_date": dt_utc_start.isoformat(),
        "end_date": dt_utc_end.isoformat(),
        "time_trunc": "hour"
    }

    print(f"⏳ Fetching {dt_local.date()}...")
    try:
        res = requests.get(BASE_URL, headers=HEADERS, params=params)
        res.raise_for_status()
        values = res.json().get("indicator", {}).get("values", [])
        if values:
            df = pd.DataFrame(values)
            df.to_csv(filename, index=False)
            print(f"✅ Saved: {filename} ({len(df)} rows)")
        else:
            print(f"⚠️ No data for {dt_local.date()}")
    except Exception as e:
        print(f"❌ Error on {dt_local.date()}: {e}")
        time.sleep(5)  # Wait in case of rate limit
