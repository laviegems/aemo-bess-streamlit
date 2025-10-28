import zipfile, sys, io, pandas as pd

p = sys.argv[1]
with zipfile.ZipFile(p, "r") as z:
    csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
    name = csvs[0]
    raw = z.read(name)
    df = pd.read_csv(io.BytesIO(raw), engine="python", sep=None, header=None, dtype=str)
    print(df.head(10).to_string(index=False))
    # find header row (where col4 == 'SETTLEMENTDATE')
    hdr_idx = df.index[(df.iloc[:,4].str.upper() == "SETTLEMENTDATE")].tolist()
    if not hdr_idx:
        print("No header row found with SETTLEMENTDATE in col4")
        sys.exit(1)
    h = hdr_idx[0]
    data = df.iloc[h+1:, [0,4,5,6,7]].copy()
    data.columns = ["C","SETTLEMENTDATE","DUID","SCADAVALUE","LASTCHANGED"]
    data = data[data["C"]=="D"]
    data["SCADAVALUE"] = pd.to_numeric(data["SCADAVALUE"], errors="coerce")
    data["timestamp"] = pd.to_datetime(data["SETTLEMENTDATE"], errors="coerce")
    out = data[["timestamp","DUID","SCADAVALUE"]].dropna()
    print("\nParsed sample:")
    print(out.head().to_string(index=False))
