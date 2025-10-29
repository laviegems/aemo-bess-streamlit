import os, pandas as pd, streamlit as st
from pathlib import Path

# ---- AI Summary panel (reads newest Markdown if present) ----
import datetime as dt

rep_dir = Path("data/reports")
latest_rep = None
if rep_dir.exists():
    reports = sorted(rep_dir.glob("report_*.md"))
    if reports:
        latest_rep = reports[-1]

with st.expander("📄 AI Daily Summary (latest)", expanded=True):
    if latest_rep and latest_rep.exists():
        st.markdown(latest_rep.read_text(encoding="utf-8"))
        st.caption(f"Source: {latest_rep.name}")
    else:
        st.info("No summary report yet. It will appear after the next workflow run.")


st.set_page_config(page_title="AEMO 5-min SCADA MW", layout="wide")

# ✅ Safe optional auto-refresh (works only if your Streamlit version supports it)
if hasattr(st, "autorefresh"):
    st.autorefresh(interval=300_000, key="auto_refresh_5min")  # 5 minutes

DATA_DIR = os.getenv("AEMO_DATA_DIR", "data/aemo")
st.title("AEMO 5-min MW Performance — Per-DUID view")


def load_all():
    files = sorted(Path(DATA_DIR).glob("aemo_*_*_5min.csv"))
    if not files:
        return pd.DataFrame(columns=["timestamp","duid","power_MW","day"])
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=["timestamp"])
        df["day"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

df = load_all()
if df.empty:
    st.warning("No data yet. Fetch once locally or wait for the daily job.")
    st.stop()

# Always default to the latest day present in files
days = sorted(df["day"].unique())
latest_idx = len(days) - 1
colA, colB = st.columns(2)
day = colA.selectbox("Day", days, index=latest_idx)
duids = sorted(df["duid"].unique())
picked = colB.multiselect("DUID(s)", duids, default=[duids[0]])

view = df[(df["day"] == day) & (df["duid"].isin(picked))]
st.write(f"**{day}** — rows: {len(view):,}")

for d in picked:
    sub = view[view["duid"] == d]
    with st.expander(f"{d} — KPIs & Chart", expanded=False):
        if sub.empty:
            st.info("No rows.")
            continue
        kpi = sub.agg(
            power_min=("power_MW","min"),
            power_max=("power_MW","max"),
            power_mean=("power_MW","mean"),
            energy_MWh=("power_MW", lambda s: (s.sum()*5/60.0))
        )
        st.dataframe(kpi, use_container_width=True, height=90)
        st.line_chart(sub.set_index("timestamp")["power_MW"])

# ---- Forecast panel (next-day) ----
from pathlib import Path as _Path
import pandas as _pd

_fdir = _Path("data/forecast")
_latest_fore = None
if _fdir.exists():
    _cand = sorted(_fdir.glob("forecast_*_nextday.csv"))
    if _cand:
        _latest_fore = _cand[-1]

with st.expander("🔮 Next-day Forecast (per DUID)", expanded=False):
    if _latest_fore:
        fdf = _pd.read_csv(_latest_fore, parse_dates=["timestamp"])
        day_next = fdf["timestamp"].dt.strftime("%Y-%m-%d").min()
        st.caption(f"Forecast day: {day_next}  ·  Source: {_latest_fore.name}")
        for d in picked:
            subf = fdf[fdf["duid"] == d]
            if subf.empty:
                st.info(f"No forecast for {d}.")
                continue
            st.write(f"**{d}** forecast (MW)")
            st.line_chart(subf.set_index("timestamp")["power_hat_MW"])
    else:
        st.info("No forecast file yet. It appears after the daily workflow runs.")

# ---- Forecast ramp alerts ----
_ra = None
if _fdir.exists():
    _cand = sorted(_fdir.glob("ramp_alerts_*_nextday.csv"))
    if _cand:
        _ra = _cand[-1]

with st.expander("⚠️ Predicted Ramp Alerts (next day)", expanded=False):
    if _ra:
        radf = _pd.read_csv(_ra, parse_dates=["timestamp"])
        show = radf[radf["duid"].isin(picked)].copy()
        if not show.empty:
            st.dataframe(show.sort_values(["duid","timestamp"]), use_container_width=True)
        else:
            st.info("No predicted ramps for selected DUIDs.")
    else:
        st.info("No ramp-alert file yet.")

