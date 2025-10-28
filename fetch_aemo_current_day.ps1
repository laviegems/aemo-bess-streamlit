param(
  [Parameter(Mandatory=$true)][string]$Date,      # e.g. 2025-10-27
  [Parameter(Mandatory=$true)][string]$Duids,     # e.g. "CLUNY,AGLSOM"
  [string]$OutDir = "data\aemo"
)

# --- constants
$root    = "https://www.nemweb.com.au"
$baseRel = "/REPORTS/CURRENT/Dispatch_SCADA/"
$dirUrl  = "$root$baseRel"

# Check date and DUIDs
Write-Host "DEBUG Date=[$Date]  Duids=[$Duids]"
$ErrorActionPreference = "Stop"


# TLS and folders
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$yyyymmdd = (Get-Date $Date).ToString("yyyyMMdd")
$tmp = Join-Path $env:TEMP "aemo_$yyyymmdd"
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Write-Host "Listing $dirUrl ..."
$resp = Invoke-WebRequest -Uri $dirUrl -UseBasicParsing

# Find all zip names for the day (don’t rely on <a> tags; scrape text)
$pattern = "PUBLIC_DISPATCHSCADA_${yyyymmdd}\d{4}_[\d]+\.zip"
$names = [regex]::Matches($resp.Content, $pattern) | ForEach-Object { $_.Value } | Sort-Object -Unique
if (-not $names) { throw "No CURRENT files for $Date at $dirUrl" }

# Build absolute URLs correctly (avoid double paths)
$dirUri = [Uri]$dirUrl
$urls = foreach ($name in $names) {
  if ($name -match '^https?://') {
    $name
  } elseif ($name.StartsWith("/")) {
    "$root$name"
  } else {
    ([Uri]::new($dirUri, $name)).AbsoluteUri
  }
}

# Download with basic retry
function Download-WithRetry($url, $dest) {
  for ($i=1; $i -le 5; $i++) {
    try {
      Invoke-WebRequest -Uri $url -OutFile $dest -TimeoutSec 60
      return
    } catch {
      if ($i -eq 5) { throw }
      Start-Sleep -Seconds ([int][math]::Min(30, 2*$i))
    }
  }
}

# Fetch all zips
foreach ($u in $urls) {
  $fname = Split-Path $u -Leaf
  $dest = Join-Path $tmp $fname
  if (Test-Path $dest) { continue }
  Write-Host "↓ $u"
  Download-WithRetry -url $u -dest $dest
}

# Stitch + filter to your DUIDs
$duidsArg = $Duids
python src\stitch_dispatch_scada.py --zips "$tmp" --duids "$duidsArg" --out (Join-Path $OutDir "aemo_$Date_CLUNY_AGLSOM_5min.csv")
Write-Host "✅ Done: $(Join-Path $OutDir "aemo_$Date_CLUNY_AGLSOM_5min.csv")"
