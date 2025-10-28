import zipfile, sys, io
from pathlib import Path
import pandas as pd

def extract_duids_from_zip(zpath: Path):
    with zipfile.ZipFile(zpath, "r") as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs: 
            return []
        raw = z.read(csvs[0])
        # read raw as no-header; sniff delimiter
        df = pd.read_csv(io.BytesIO(raw), engine="python", sep=None, header=None, dtype=str)
        # find header row where col4 == SETTLEMENTDATE
        hdr_idx = df.index[(df.iloc[:,4].str.upper() == "SETTLEMENTDATE")].tolist()
        if not hdr_idx:
            return []
        h = hdr_idx[0]
        data = df.iloc[h+1:, [0,4,5,6,7]].copy()
        data.columns = ["C","SETTLEMENTDATE","DUID","SCADAVALUE","LASTCHANGED"]
        data = data[data["C"]=="D"]
        if "DUID" not in data:
            return []
        return data["DUID"].astype(str).str.upper().tolist()

zdir = Path(sys.argv[1])
duids = []
count = 0
for zf in sorted(zdir.glob("PUBLIC_DISPATCHSCADA_*.zip")):
    count += 1
    try:
        duids.extend(extract_duids_from_zip(zf))
    except Exception as e:
        print(f"[skip] {zf.name}: {e}")

print(f"Scanned {count} zip chunks")
s = pd.Series(duids)
if s.empty:
    print("No DUIDs found (check file format or path).")
else:
    print("Top DUIDs by count:\n")
    print(s.value_counts().head(50))
