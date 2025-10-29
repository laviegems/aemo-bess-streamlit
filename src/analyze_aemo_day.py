# src/analyze_aemo_day.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
from src.agent_summary import summarize_day, render_markdown

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="Path to aemo_YYYY-MM-DD_*.csv. If omitted, use newest in data/aemo.")
    ap.add_argument("--outdir", default="data/reports", help="Output dir for report.md and report.json")
    args = ap.parse_args()

    if args.file:
        f = Path(args.file)
    else:
        cand = sorted(Path("data/aemo").glob("aemo_*_*_5min.csv"))
        if not cand:
            raise SystemExit("No daily CSVs in data/aemo.")
        f = cand[-1]

    df = pd.read_csv(f, parse_dates=["timestamp"])
    sums = summarize_day(df)
    md = render_markdown(sums)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    # infer day from file content (safer)
    day = df["timestamp"].dt.strftime("%Y-%m-%d").iloc[0]
    md_path = outdir / f"report_{day}.md"
    json_path = outdir / f"report_{day}.json"

    with open(md_path, "w", encoding="utf-8") as fp:
        fp.write(md)

    # make a compact JSON too
    as_dict = {
        d: vars(s) for d, s in sums.items()
    }
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(as_dict, fp, ensure_ascii=False, indent=2)

    print(f"✅ wrote {md_path}")
    print(f"✅ wrote {json_path}")

if __name__ == "__main__":
    main()
