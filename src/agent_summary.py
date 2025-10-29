# src/agent_summary.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd

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
    outages: List[Tuple[str,str,int]]
    anomalies: int
    # ===== trends =====
    slope_mw_per_hr: float         # global linear trend (MW/hour)
    intraday_up_bursts: int        # # of contiguous segments with strong +slope
    intraday_down_bursts: int      # # of contiguous segments with strong -slope
    diurnal_profile: List[Tuple[int,float]]  # [(hour, mean_MW)]
    notes: List[str]

def _find_zero_runs(s: pd.Series, min_points: int = 3) -> List[Tuple[int,int]]:
    z = (s.fillna(0) == 0).astype(int)
    edges = z.diff().fillna(z.iloc[0]).ne(0)
    idx = np.flatnonzero(edges.values)
    idx = np.r_[idx, len(z)]
    runs = []
    for i in range(len(idx)-1):
        a, b = idx[i], idx[i+1]
        if z.iloc[a] == 1 and (b-a) >= min_points:
            runs.append((a, b-1))
    return runs

def _zscore_anomalies(s: pd.Series, win: int = 12, z_thr: float = 3.0) -> pd.Series:
    ds = s.diff()
    mu = ds.rolling(win, min_periods=max(3, win//2)).mean()
    sd = ds.rolling(win, min_periods=max(3, win//2)).std(ddof=0)
    z = (ds - mu) / (sd.replace(0, np.nan))
    return z.abs() > z_thr

def _global_trend(timestamp: pd.Series, p: pd.Series) -> float:
    # slope in MW/hour using linear regression on time (minutes-from-start)
    t0 = timestamp.min()
    x_min = (timestamp - t0).dt.total_seconds() / 60.0
    y = p.values.astype(float)
    if len(x_min) < 3:
        return 0.0
    x = x_min.values
    A = np.vstack([x, np.ones_like(x)]).T
    slope_mw_per_min, _ = np.linalg.lstsq(A, y, rcond=None)[0]
    return float(slope_mw_per_min * 60.0)

def _burst_counts(p: pd.Series, slope_thr_mw_per_5min: float = 10.0) -> tuple[int,int]:
    # approximate slope as 5-min first difference; count contiguous bursts
    dp = p.diff()
    up_mask = dp > slope_thr_mw_per_5min
    dn_mask = dp < -slope_thr_mw_per_5min
    def count_runs(mask: pd.Series) -> int:
        if mask.empty: return 0
        return int(((~mask.shift(fill_value=False)) & mask).sum())
    return count_runs(up_mask), count_runs(dn_mask)

def summarize_duid(df: pd.DataFrame, duid: str) -> DuidSummary:
    sub = df[df["duid"] == duid].sort_values("timestamp")
    day = sub["timestamp"].dt.strftime("%Y-%m-%d").iloc[0]
    p = sub["power_MW"].astype(float)

    dt5h = 5.0/60.0
    energy_mwh = float(np.nansum(p)*dt5h)

    ramp = p.diff().abs()
    ramp_max = float(np.nanmax(ramp))
    ramp_95p = float(np.nanpercentile(ramp.dropna(), 95)) if ramp.notna().any() else 0.0

    runs = _find_zero_runs(p, min_points=3)
    outages = []
    for a, b in runs:
        t0 = sub["timestamp"].iloc[a]; t1 = sub["timestamp"].iloc[b]
        outages.append((t0.isoformat(), t1.isoformat(), (b-a+1)))

    anom_mask = _zscore_anomalies(p, win=12, z_thr=3.0)
    anomalies = int(anom_mask.fillna(False).sum())

    # trends
    slope_mw_per_hr = _global_trend(sub["timestamp"], p)
    # dynamic slope threshold from scale of series
    dynamic_thr = max(10.0, 0.1*(np.nanmax(p)-np.nanmin(p)))
    up_bursts, down_bursts = _burst_counts(p, slope_thr_mw_per_5min=dynamic_thr)

    # diurnal hourly profile
    diurnal = sub.assign(hour=sub["timestamp"].dt.hour).groupby("hour")["power_MW"].mean().round(3)
    diurnal_profile = [(int(h), float(v)) for h, v in diurnal.items()]

    notes = []
    if (p < 0).any(): notes.append("Negative dispatch observed.")
    if ramp_max > max(20.0, 0.2*(np.nanmax(p)-np.nanmin(p))):
        notes.append(f"Large ramp detected: {ramp_max:.1f} MW/5min.")
    if outages: notes.append(f"{len(outages)} outage-like zero segments (≥15 min).")
    if anomalies > 0: notes.append(f"{anomalies} spike/step anomalies flagged.")
    if abs(slope_mw_per_hr) > 5.0:
        notes.append(f"Monotonic trend: slope {slope_mw_per_hr:+.1f} MW/h.")

    return DuidSummary(
        duid=duid, day=day, n_rows=len(sub),
        p_min=float(np.nanmin(p)), p_max=float(np.nanmax(p)), p_mean=float(np.nanmean(p)),
        energy_mwh=energy_mwh, zero_frac=float((p==0).mean()), neg_frac=float((p<0).mean()),
        ramp_max=ramp_max, ramp_95p=ramp_95p, outages=outages, anomalies=anomalies,
        slope_mw_per_hr=slope_mw_per_hr, intraday_up_bursts=up_bursts, intraday_down_bursts=down_bursts,
        diurnal_profile=diurnal_profile, notes=notes
    )

def summarize_day(df: pd.DataFrame) -> Dict[str, DuidSummary]:
    return {d: summarize_duid(df, d) for d in sorted(df["duid"].unique())}

def render_markdown(sums: Dict[str, DuidSummary]) -> str:
    lines = ["# AEMO Daily Operational Summary"]
    if not sums: return lines[0] + "\n\n_No data_"
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
            f"- Trend slope: **{s.slope_mw_per_hr:+.2f} MW/h**; Burst up/down: **{s.intraday_up_bursts}/{s.intraday_down_bursts}**",
            "- Diurnal profile (hour → mean MW): " + ", ".join([f"{h:02d}:{v:.1f}" for h,v in s.diurnal_profile]),
        ]
        if s.outages:
            total_pts = sum(n for *_ , n in s.outages)
            lines.append(f"- Outage-like zero segments: **{len(s.outages)}** (total points **{total_pts}**)")
        else:
            lines.append(f"- Outage-like zero segments: **0**")
        if s.notes:
            lines.append("- Notes: " + "; ".join(s.notes))
        lines.append("")
    return "\n".join(lines)
