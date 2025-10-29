# AEMO BESS (CLUNY & BUTLERSG) — 5‑min MW, Daily Auto-Update

This repo fetches AEMO DISPATCH_UNIT_SCADA for **CLUNY** and **BUTLERSG** (5‑minute SCADA MW)
once per day using **GitHub Actions**, writes a CSV into `data/aemo/`, and serves a **Streamlit**
dashboard (free) on **Streamlit Community Cloud**.

## How it works
- `.github/workflows/fetch_aemo.yml` runs daily and commits a new CSV:
  `data/aemo/aemo_YYYY-MM-DD_CLUNY_BUTLERSG_5min.csv`
- `app/streamlit_bess.py` loads all CSVs and renders a dashboard with per‑unit KPIs and charts.

## Quick start
1. Create a new GitHub repo and upload the contents of this ZIP.
2. Enable **Actions** in the repo (it may run automatically).
3. Go to **streamlit.io → Community Cloud → New app**:
   - Repo: _your repo_
   - File: `app/streamlit_bess.py`
   - Python: 3.12
4. After the Action runs, your app will show the data.

## Local test
```bash
pip install -r requirements.txt
python src/fetch_aemo_duids_day.py --day 2025-07-15 --duids CLUNY,BUTLERSG --outdir data/aemo
streamlit run app/streamlit_bess.py --server.port 8501
```
