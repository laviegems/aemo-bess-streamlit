from __future__ import annotations
import io, zipfile, re
from pathlib import Path
from typing import Iterable, List
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ARCHIVE_BASE = "https://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA"
CURRENT_BASE = "https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA"
UA = {"User-Agent": "aemo-fetcher/1.2 (python-requests)"}

# ---------- HTTP utils ----------
def make_session(retries: int = 5, backoff: float = 0.5) -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=retries, connect=retries, read=retries, status=retries,
        backoff_factor=backoff, status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","HEAD"], raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=r, pool_connections=10, pool_maxsize=10)
    s.mount("https://", ad); s.mount("http://", ad)
    return s

def get_bytes(url: str, sess: requests.Session, timeout=(8, 30)) -> bytes:
    print(f"→ fetching {url}")
    resp = sess.get(url, headers=UA, timeout=timeout, stream=False)
    if resp.status_code == 404:
        print(f"⚠️ 404: {url}")
        raise FileNotFoundError(url)
    resp.raise_for_status()
    return resp.content

# ---------- Banner CSV parser ----------
def parse_banner_zip_bytes(raw_zip: bytes) -> pd.DataFrame:
    """Return dataframe with columns: timestamp, duid, power_MW (parsed from 'banner' CSV)."""
    z = zipfile.ZipFile(io.BytesIO(raw_zip))
    names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not names:
        return pd.DataFrame()
    raw_csv = z.read(names[0])
    df = pd.read_csv(io.BytesIO(raw_csv), engine="python", sep=None, header=None, dtype=str)
    # header row: column 4 equals 'SETTLEMENTDATE'
    idx = df.index[(df.iloc[:,4].str.upper() == "SETTLEMENTDATE")].tolist()
    if not idx:
        return pd.DataFrame()
    h = idx[0]
    data = df.iloc[h+1:, [0,4,5,6,7]].copy()
    data.columns = ["C", "SETTLEMENTDATE", "DUID", "SCADAVALUE", "LASTCHANGED"]
    data = data[data["C"] == "D"]
    if data.empty:
        return pd.DataFrame()
    data["timestamp"] = pd.to_datetime(data["SETTLEMENTDATE"], errors="coerce")
    data["power_MW"] = pd.to_numeric(data["SCADAVALUE"], errors="coerce")
    data["duid"] = data["DUID"].astype(str).str.upper()
    return data.loc[data["timestamp"].notna(), ["timestamp","duid","power_MW"]]

# ---------- Fetchers ----------
def fetch_archive_day_df(yyyymmdd: str, sess: requests.Session) -> pd.DataFrame:
    url = f"{ARCHIVE_BASE}/PUBLIC_DISPATCHSCADA_{yyyymmdd}.zip"
    try:
        b = get_bytes(url, sess)
    except FileNotFoundError:
        return pd.DataFrame()
    return parse_banner_zip_bytes(b)

def list_current_day_urls(yyyymmdd: str, sess: requests.Session) -> List[str]:
    idx = sess.get(f"{CURRENT_BASE}/", headers=UA, timeout=(8,45))
    idx.raise_for_status()
    pat = re.compile(rf"PUBLIC_DISPATCHSCADA_{yyyymmdd}\d{{4}}_[\d]+\.zip", re.I)
    names = sorted(set(pat.findall(idx.text)))
    return [f"{CURRENT_BASE}/{n}" for n in names]

def fetch_current_day_df(yyyymmdd: str, sess: requests.Session) -> pd.DataFrame:
    urls = list_current_day_urls(yyyymmdd, sess)
    parts: List[pd.DataFrame] = []
    for u in urls:
        try:
            b = get_bytes(u, sess)
            df = parse_banner_zip_bytes(b)
            if not df.empty: parts.append(df)
        except Exception:
            continue
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def filter_duids(df: pd.DataFrame, duids: Iterable[str]) -> pd.DataFrame:
    want = {d.strip().upper() for d in duids if d.strip()}
    if not want or "*" in want: return df
    return df[df["duid"].isin(want)]
