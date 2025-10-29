# src/agent_summary.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

@dataclass
class DuidSummary:
    duid: str
    day: str
    n_rows: int
    p_min: float
    p_max: float
    p_mean: float
    energy_mwh: float
    zero_frac: float
    neg_frac: float
    ramp_max: float
    ramp_95p: float
    outages: List[Tuple[str,str,int]]  # (start_iso, end_iso, points)
    anomalies: int
    notes: List[str]

def _find_zero_runs(s: pd.Series, min_points: int = 3) -> List[Tuple[int,int]]:
    """Return zero-power run (inclusive index ranges) >= min_points."""
    z = (s.fillna(0) == 0).astype(int)
    # boundaries
    edges = z.diff().fillna(z.iloc[0]).ne(0)
    idx = np.flatnonzero(edges.values)
    # add end sentinel
    idx = np.r_[idx, len(z)]
    runs = []
    start = 0
    for i in range(len(idx)-1):
        a, b = idx[i], idx[i+1]
        if z.iloc[a] == 1 and (b - a) >= min_points:
            runs.append((a, b-1))
        start = b
    return runs

def _zscore_anomalies(s: pd.Series, win: int = 12, z_thr: float = 3.0) -> pd.Series:
    """Rolling z-score on first differences to catch spikes/steps."""
    ds = s.diff()
    mu = ds.rolling(win, min_periods=max(3, win//2)).mean()
    sd = ds.rolling(win, min_periods=max(3, win//2)).std(ddof=0)
    z = (ds - mu) / (sd.replace(0, np.nan))
    return z.abs() > z_thr

def summarize_duid(df: pd.DataFrame, duid: str) -> DuidSummary:
    sub = df[df["duid"] == duid].sort_values("timestamp")
    day = sub["timestamp"].dt.strftime("%Y-%m-%d").iloc[0]
    p = sub["power_MW"].astype(float)
    dt5h = 5.0/60.0
    energy_mwh = np.nansum(p)*dt5h

    # ramps (MW/5min)
    ramp = p.diff().abs()
    ramp_max = float(np.nanmax(ramp))
    ramp_95p = float(np.nanpercentile(ramp.dropna(), 95)) if ramp.notna().any() else 0.0

    # “outages” = sustained zeros (>= 3 ticks = 15 min)
    runs = _find_zero_runs(p, min_points=3)
    outages = []
    for a, b in runs:
        t0 = sub["timestamp"].iloc[a]
        t1 = sub["timestamp"].iloc[b]
        outages.append((t0.isoformat(), t1.isoformat(), (b-a+1)))

    # anomalies via rolling z on deltas
    anom_mask = _zscore_anomalies(p, win=12, z_thr=3.0)
    anomalies = int(anom_mask.fillna(False).sum())

    notes = []
    if (p < 0).any():
        notes.append("Negative dispatch observed.")
    if ramp_max > max(20.0, 0.2*(np.nanmax(p)-np.nanmin(p))):
        notes.append(f"Large ramp detected: {ramp_max:.1f} MW/5min.")
    if outages:
        notes.append(f"{len(outages)} outage-like zero segments (≥15 min).")
    if anomalies > 0:
        notes.append(f"{anomalies} spike/step anomalies flagged.")

    return DuidSummary(
        duid=duid, day=day, n_rows=len(sub),
        p_min=float(np.nanmin(p)), p_max=float(np.nanmax(p)), p_mean=float(np.nanmean(p)),
        energy_mwh=float(energy_mwh),
        zero_frac=float((p==0).mean()), neg_frac=float((p<0).mean()),
        ramp_max=ramp_max, ramp_95p=ramp_95p,
        outages=outages, anomalies=anomalies, notes=notes
    )

def summarize_day(df: pd.DataFrame) -> Dict[str, DuidSummary]:
    out: Dict[str,DuidSummary] = {}
    for d in sorted(df["duid"].unique()):
        out[d] = summarize_duid(df, d)
    return out

def render_markdown(sums: Dict[str, DuidSummary]) -> str:
    lines = ["# AEMO Daily Operational Summary"]
    if not sums:
        return "# AEMO Daily Operational Summary\n\n_No data_"
    day = next(iter(sums.values())).day
    lines.append(f"**Day:** {day}\n")
    for d, s in sums.items():
        lines += [
            f"## {d}",
            f"- Rows: {s.n_rows}",
            f"- Power (MW): min **{s.p_min:.2f}**, mean **{s.p_mean:.2f}**, max **{s.p_max:.2f}**",
            f"- Energy: **{s.energy_mwh:.2f} MWh**",
            f"- Zero-output fraction: **{100*s.zero_frac:.1f}%**, Negative fraction: **{100*s.neg_frac:.2f}%**",
            f"- Ramps (|ΔMW|/5min): 95th **{s.ramp_95p:.2f}**, max **{s.ramp_max:.2f}**",
        ]
        if s.outages:
            total_pts = sum(n for *_ , n in s.outages)
            lines.append(f"- Outage-like zero segments: **{len(s.outages)}** (total points **{total_pts}**)")
        else:
            lines.append(f"- Outage-like zero segments: **0**")
        if s.notes:
            lines.append("- Notes: " + "; ".join(s.notes))
        lines.append("")  # blank line
    return "\n".join(lines)
