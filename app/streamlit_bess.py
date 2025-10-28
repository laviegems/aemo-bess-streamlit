import os, pandas as pd, streamlit as st
from pathlib import Path

st.set_page_config(page_title="AEMO 5-min SCADA MW", layout="wide")

# ✅ Safe optional auto-refresh (works only if your Streamlit version supports it)
if hasattr(st, "autorefresh"):
    st.autorefresh(interval=300_000, key="auto_refresh_5min")  # 5 minutes

DATA_DIR = os.getenv("AEMO_DATA_DIR", "data/aemo")
st.title("AEMO 5-min MW — Per-DUID view")


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
