import argparse, zipfile, io
from pathlib import Path
import pandas as pd

def read_banner_csv_from_zip(zpath: Path) -> pd.DataFrame:
    """Parse AEMO banner format: keep only rows with C=='D'; take cols 4:8."""
    with zipfile.ZipFile(zpath, "r") as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs: 
            return pd.DataFrame()
        raw = z.read(csvs[0])
        df = pd.read_csv(io.BytesIO(raw), engine="python", sep=None, header=None, dtype=str)
        # find the header row (col4 == 'SETTLEMENTDATE')
        hdr_idx = df.index[(df.iloc[:,4].str.upper() == "SETTLEMENTDATE")].tolist()
        if not hdr_idx:
            return pd.DataFrame()
        h = hdr_idx[0]
        data = df.iloc[h+1:, [0,4,5,6,7]].copy()
        data.columns = ["C","SETTLEMENTDATE","DUID","SCADAVALUE","LASTCHANGED"]
        data = data[data["C"]=="D"]
        data.drop(columns=["C","LASTCHANGED"], inplace=True)
        # types
        data["SCADAVALUE"] = pd.to_numeric(data["SCADAVALUE"], errors="coerce")
        data["timestamp"]  = pd.to_datetime(data["SETTLEMENTDATE"], errors="coerce")
        data = data.dropna(subset=["timestamp"])
        data.rename(columns={"DUID":"duid","SCADAVALUE":"power_MW"}, inplace=True)
        return data[["timestamp","duid","power_MW"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zips", required=True, help="Folder with PUBLIC_DISPATCHSCADA_*.zip")
    ap.add_argument("--duids", required=True, help='Comma list "DUID1,DUID2" or "*" for all')
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    want = [d.strip().upper() for d in args.duids.split(",") if d.strip()]
    want_all = (len(want)==1 and want[0]=="*")

    zips = sorted(Path(args.zips).glob("PUBLIC_DISPATCHSCADA_*.zip"))
    if not zips:
        raise SystemExit("No zip chunks found in --zips folder.")

    parts, seen = [], set()
    for z in zips:
        df = read_banner_csv_from_zip(z)
        if df.empty: 
            continue
        df["duid"] = df["duid"].str.upper()
        seen.update(df["duid"].unique())
        if not want_all:
            df = df[df["duid"].isin(want)]
        if not df.empty:
            parts.append(df)

    print(f"Unique DUIDs seen (first 20): {sorted(list(seen))[:20]}")
    if not parts:
        raise SystemExit("No rows for requested DUIDs. Try --duids '*' to export everything, then choose.")
    out = pd.concat(parts, ignore_index=True).sort_values(["duid","timestamp"])
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"✅ wrote {args.out} rows={len(out):,}")

if __name__ == "__main__":
    main()
