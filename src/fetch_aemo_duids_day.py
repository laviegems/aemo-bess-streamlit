from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from src.aemo_banner import (   
    make_session, fetch_archive_day_df, fetch_current_day_df, filter_duids
)

def fetch_day(day: str, duids: list[str], source: str = "auto") -> pd.DataFrame:
    ts = pd.to_datetime(day); yyyymmdd = ts.strftime("%Y%m%d")
    sess = make_session()

    df = pd.DataFrame()
    if source in ("auto","archive"):
        df = fetch_archive_day_df(yyyymmdd, sess)
    if df.empty and source in ("auto","current"):
        df = fetch_current_day_df(yyyymmdd, sess)
    if df.empty:
        raise FileNotFoundError(f"No AEMO DISPATCH_SCADA rows for {day} (archive+current empty).")

    df = filter_duids(df, duids)
    if df.empty:
        raise ValueError(f"No rows for requested DUIDs {duids} on {day}.")
    return df.sort_values(["duid","timestamp"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (NEM local date)")
    ap.add_argument("--duids", required=True, help='Comma list "DUID1,DUID2" or "*" for all')
    ap.add_argument("--outdir", default="data/aemo")
    ap.add_argument("--source", choices=["auto","archive","current"], default="auto")
    args = ap.parse_args()

    duids = [d.strip() for d in args.duids.split(",")]
    df = fetch_day(args.day, duids, source=args.source)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    duid_tag = "ALL" if duids == ["*"] else "_".join([d.upper() for d in duids])
    out = outdir / f"aemo_{args.day}_{duid_tag}_5min.csv"
    df.to_csv(out, index=False)
    print(f"✅ wrote {out} rows={len(df):,}")

if __name__ == "__main__":
    main()
