## Collection migration - 1 October 2026

Active collection has moved to the grouped five-curve workflow in [Energy Data Collection](https://github.com/Amir-Torbati/ree-data-platform/blob/main/docs/FUNDAMENTALS.md) (owner access required). It collects verified ESIOS indicators 1295 (actual PV), 551 (actual wind), 1293 (actual demand), 542 (PV forecast) and 541 (wind forecast).

**The files below are a preserved legacy archive, not certified actual-generation data.** The review found source-ID mismatches in the solar/wind collectors and reliability problems in the demand scraper. Current code alone does not establish the origin of every historical file. Do not combine this archive with the corrected curves without checking provenance. Legacy collection/backfill workflows are retired to prevent overlapping or mislabelled downloads. Data files and Git history have not been deleted or relabelled.

---

☀️ Hourly Solar PV Generation – Spain (Hourly Resolution)
This project collects and stores real-time **hourly solar photovoltaic (PV) generation data** for the Peninsular region of Spain using the official Red Eléctrica de España (ESIOS) API.

We built a fully automated data pipeline that updates every hour and stores results in structured daily files — ready for analysis, modeling, or dashboards.

---

📁 Folder Structure

**data/**  
→ Daily CSV files saved as `YYYY-MM-DD-hourly.csv`.

**scripts/**  
→ Python script for fetching hourly solar data from ESIOS.

**.github/workflows/**  
→ GitHub Actions automation (runs every hour).

---

🔄 What’s Automated?

✅ **Real-Time Fetching** (`collect_solar_hourly.yml`)  
- Pulls hourly PV generation via ESIOS API  
- Runs **every hour** using GitHub Actions  
- Saves updated CSV file in `data/YYYY-MM-DD-hourly.csv`  
- Avoids duplicates, merges safely  

✅ **Timezone-Aware Storage**  
- Tracks Spain local calendar (Europe/Madrid)  
- Converts timestamps to UTC for consistency  
- File is named by **local date** (e.g. `2025-04-14-hourly.csv`)

---

🧠 Scripts

**collect_today_hourly.py**  
- Converts local Spain time to UTC  
- Sends API request using `time_trunc = "hour"`  
- Loads today's CSV (if it exists)  
- Appends new hourly records  
- Deduplicates by timestamp  
- Saves updated result

---

🛠 Tech Stack

- Python 3.11  
- Pandas  
- Requests  
- GitHub Actions  
- Red Eléctrica de España (ESIOS API)

---

📡 Data Source

- **Provider**: Red Eléctrica de España (REE)  
- **API**: https://api.esios.ree.es/  
- **Indicator**: `541` (Solar PV Generation - Peninsular)  
- **Timezone**: Timestamps saved in UTC, files named in Spain time

---

🗺️ Roadmap

✅ Collect hourly solar PV generation  
✅ Store daily files in `data/` folder  
✅ GitHub Actions automation  
🔜 Add Parquet + DuckDB versions  
🔜 Backfill data from 2023  
🔜 Add real-time dashboard (Streamlit or Looker)  
🔜 Merge with weather + OMIE price data  
🔜 BESS optimization models using solar input

---

👤 Author

Created with ☀️ by **Amir Torbati**  
All rights reserved © 2025

Please cite or credit if you use this project for academic, research, or commercial purposes.

---

⚡ Let the sunlight power your models.
