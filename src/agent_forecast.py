# src/agent_forecast.py
from __future__ import annotations
import argparse
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd

@dataclass
class ForecastConfig:
    alpha: float = 0.3
    ramp_alert_sigma: float = 2.0

def forecast_series(y: pd.Series, alpha: float) -> pd.Series:
    y = pd.to_numeric(y, errors="coerce").fillna(method="ffill").fillna(0.0)
    yhat = [y.iloc[0]]
    for i in range(1, len(y)):
        yhat.append(alpha * y.iloc[i-1] + (1-alpha) * yhat[-1])
    return pd.Series(yhat, index=y.index)

def forecast_next_day(df: pd.DataFrame, cfg: ForecastConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    out_rows, alert_rows = [], []
    df = df.sort_values(["duid","timestamp"])
    if df.empty:
        # return valid empty frames with headers
        return (
            pd.DataFrame(columns=["timestamp","duid","power_hat_MW"]),
            pd.DataFrame(columns=["timestamp","duid","predicted_ramp_MW"])
        )

    day0 = df["timestamp"].min().floor("D")
    idx_next = pd.date_range(day0 + pd.Timedelta(days=1),
                             day0 + pd.Timedelta(days=1, minutes=5*287),
                             freq="5min")

    for duid, sub in df.groupby("duid", as_index=False):
        p = pd.to_numeric(sub["power_MW"], errors="coerce").fillna(method="ffill").fillna(0.0).reset_index(drop=True)
        if len(p) == 0:
            continue

        yhat_in = forecast_series(p, cfg.alpha)
        last_hat = yhat_in.iloc[-1]
        mean_level = float(p.mean())

        ph, ht = [], float(last_hat)
        for _ in range(len(idx_next)):
            ht = cfg.alpha*mean_level + (1-cfg.alpha)*ht
            ph.append(ht)

        fduid = pd.DataFrame({"timestamp": idx_next, "duid": duid, "power_hat_MW": ph})
        out_rows.append(fduid)

        # ramp alert threshold from historical deltas (guard zeros/NaNs)
        deltas = p.diff()
        ramp_sd = float(deltas.std(ddof=0)) if deltas.notna().any() else 0.0
        ramp_thr = cfg.ramp_alert_sigma * ramp_sd
        if ramp_thr > 0:
            dph = np.abs(np.diff(ph, prepend=ph[0]))
            alert_idx = np.where(dph >= ramp_thr)[0]
            if alert_idx.size:
                alert_rows.append(pd.DataFrame({
                    "timestamp": idx_next[alert_idx],
                    "duid": duid,
                    "predicted_ramp_MW": dph[alert_idx]
                }))

    forecast_df = (pd.concat(out_rows, ignore_index=True)
                   if out_rows else pd.DataFrame(columns=["timestamp","duid","power_hat_MW"]))
    ramp_alerts = (pd.concat(alert_rows, ignore_index=True)
                   if alert_rows else pd.DataFrame(columns=["timestamp","duid","predicted_ramp_MW"]))
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
    cfg = ForecastConfig(alpha=args.alpha, ramp_alert_sigma=args.ramp_sigma)
    forecast_df, ramp_alerts = forecast_next_day(df, cfg)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    day = df["timestamp"].dt.strftime("%Y-%m-%d").iloc[0]
    f_csv = outdir / f"forecast_{day}_nextday.csv"
    r_csv = outdir / f"ramp_alerts_{day}_nextday.csv"

    # Always write files, even if empty → include headers so readers don’t choke
    forecast_df.to_csv(f_csv, index=False)
    ramp_alerts.to_csv(r_csv, index=False)

    print(f"✅ wrote {f_csv} rows={len(forecast_df):,}")
    print(f"✅ wrote {r_csv} rows={len(ramp_alerts):,}")

if __name__ == "__main__":
    main()
