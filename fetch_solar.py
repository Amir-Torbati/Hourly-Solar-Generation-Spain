import requests
import pandas as pd
from datetime import datetime, timedelta
import os

API_TOKEN = "478a759c0ef1ce824a835ddd699195ff0f66a9b5ae3b477e88a579c6b7ec47c5"
BASE_URL = "https://api.esios.ree.es/indicators/541"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": API_TOKEN,
}

now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
one_hour_ago = now - timedelta(hours=1)

start_date = one_hour_ago.isoformat() + "Z"
end_date = now.isoformat() + "Z"

params = {
    "start_date": start_date,
    "end_date": end_date,
    "time_trunc": "hour"
}

res = requests.get(BASE_URL, headers=HEADERS, params=params)
data = res.json()["indicator"]["values"]

df_new = pd.DataFrame(data)
df_new["datetime"] = pd.to_datetime(df_new["datetime"])

file_path = "data/solar_database.csv"
if os.path.exists(file_path):
    df_existing = pd.read_csv(file_path, parse_dates=["datetime"])
    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["datetime"]).sort_values("datetime")
else:
    df_combined = df_new

os.makedirs("data", exist_ok=True)
df_combined.to_csv(file_path, index=False)
