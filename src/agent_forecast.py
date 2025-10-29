# src/agent_forecast.py
from __future__ import annotations
import argparse
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd

@dataclass
class ForecastConfig:
    alpha: float = 0.3                      # smoothing factor
    ramp_alert_sigma: float = 2.0           # ramp alert threshold (in SD of historical 5-min deltas)

def forecast_series(y: pd.Series, alpha: float) -> pd.Series:
    # simple exponential smoothing one-step-ahead
    y = y.astype(float)
    yhat = [y.iloc[0]]
    for i in range(1, len(y)):
        yhat.append(alpha * y.iloc[i-1] + (1-alpha) * yhat[-1])
    return pd.Series(yhat, index=y.index)

def forecast_next_day(df: pd.DataFrame, cfg: ForecastConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (forecast_df, ramp_alerts)
    forecast_df columns: timestamp, duid, power_hat_MW
    ramp_alerts columns: timestamp, duid, predicted_ramp_MW
    """
    out_rows = []
    alert_rows = []
    by_duid = df.sort_values("timestamp").groupby("duid", as_index=False)
    day = df["timestamp"].dt.strftime("%Y-%m-%d").iloc[0]
    # Create next-day index at same 5-min cadence (288 points)
    base = df["timestamp"].dt.floor("D").iloc[0]
    next_day = base + pd.Timedelta(days=1)
    idx_next = pd.date_range(next_day, next_day + pd.Timedelta(minutes=5*287), freq="5min")

    for duid, sub in by_duid:
        p = sub["power_MW"].astype(float).reset_index(drop=True)
        # fit smoothing on the day’s data, then roll forward using last smoothed state
        yhat_in = forecast_series(p, cfg.alpha)
        last_hat = yhat_in.iloc[-1]
        # naive forward: use last_hat as baseline, then slowly revert toward mean
        mean_level = p.mean()
        # simple AR(1)-like roll: hat_{t+1} = alpha*mean + (1-alpha)*hat_t
        ph = []
        ht = last_hat
        for _ in range(len(idx_next)):
            ht = cfg.alpha*mean_level + (1-cfg.alpha)*ht
            ph.append(ht)
        fduid = pd.DataFrame({"timestamp": idx_next, "duid": duid, "power_hat_MW": ph})
        out_rows.append(fduid)

        # ramp alert threshold from historical deltas
        ramp_sd = sub["power_MW"].diff().std(ddof=0)
        ramp_thr = float(cfg.ramp_alert_sigma * (ramp_sd if pd.notna(ramp_sd) and ramp_sd>0 else 0.0))
        if ramp_thr > 0:
            dph = np.abs(np.diff(ph, prepend=ph[0]))
            alert_idx = np.where(dph >= ramp_thr)[0]
            if alert_idx.size:
                alert_rows.append(pd.DataFrame({
                    "timestamp": idx_next[alert_idx],
                    "duid": duid,
                    "predicted_ramp_MW": dph[alert_idx]
                }))

    forecast_df = pd.concat(out_rows, ignore_index=True) if out_rows else pd.DataFrame()
    ramp_alerts = pd.concat(alert_rows, ignore_index=True) if alert_rows else pd.DataFrame()
    return forecast_df, ramp_alerts

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="aemo_YYYY-MM-DD_*.csv (if omitted uses newest in data/aemo)")
    ap.add_argument("--outdir", default="data/forecast")
    ap.add_argument("--alpha", type=float, default=0.3)
    ap.add_argument("--ramp_sigma", type=float, default=2.0)
    args = ap.parse_args()

    if args.file:
        f = Path(args.file)
    else:
        cand = sorted(Path("data/aemo").glob("aemo_*_*_5min.csv"))
        if not cand:
            raise SystemExit("No daily CSVs in data/aemo.")
        f = cand[-1]

    df = pd.read_csv(f, parse_dates=["timestamp"])
    df = df.sort_values(["duid","timestamp"])
    cfg = ForecastConfig(alpha=args.alpha, ramp_alert_sigma=args.ramp_sigma)
    forecast_df, ramp_alerts = forecast_next_day(df, cfg)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    day = df["timestamp"].dt.strftime("%Y-%m-%d").iloc[0]
    f_csv = outdir / f"forecast_{day}_nextday.csv"
    r_csv = outdir / f"ramp_alerts_{day}_nextday.csv"
    forecast_df.to_csv(f_csv, index=False)
    ramp_alerts.to_csv(r_csv, index=False)
    print(f"✅ wrote {f_csv} rows={len(forecast_df):,}")
    print(f"✅ wrote {r_csv} rows={len(ramp_alerts):,}")

if __name__ == "__main__":
    main()
