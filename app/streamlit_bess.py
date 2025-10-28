import os, pandas as pd, streamlit as st
from pathlib import Path

DATA_DIR = os.getenv("AEMO_DATA_DIR", "data/aemo")
st.set_page_config(page_title="AEMO 5-min SCADA MW", layout="wide")
st.title("AEMO 5-min MW — Per-DUID view")

def load_all():
    files = sorted(Path(DATA_DIR).glob("aemo_*_*_5min.csv"))  # any duid tag
    if not files: return pd.DataFrame(columns=["timestamp","duid","power_MW"])
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

days = sorted(df["day"].unique())
colA, colB = st.columns(2)
day = colA.selectbox("Day", days, index=len(days)-1)
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
            energy_MWh=("power_MW", lambda s: (s.sum()*5/60.0))  # 5-min to MWh
        )
        st.dataframe(kpi, use_container_width=True, height=90)
        st.line_chart(sub.set_index("timestamp")["power_MW"])
