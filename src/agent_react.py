# src/agent_react.py
from __future__ import annotations
import os, json, datetime as dt, logging
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import numpy as np

# ---------- Config ----------
MAX_TOKENS_PER_DAY = 5000
MAX_BUDGET_USD_PER_MONTH = 1.0
MODEL_NAME = "gpt-4o-mini"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("agent_react")

def _exists(p: Path) -> bool:
    try:
        return p.exists()
    except Exception:
        return False

def load_latest_analysis() -> Dict[str, Any]:
    reports = sorted(Path("data/reports").glob("report_*.json"))
    if not reports:
        log.warning("No analysis JSON found in data/reports.")
        return {}
    f = reports[-1]
    log.info(f"Using analysis JSON: {f.name}")
    with open(f, "r", encoding="utf-8") as fp:
        return json.load(fp)

def load_latest_forecast() -> pd.DataFrame:
    cand = sorted(Path("data/forecast").glob("forecast_*_nextday.csv"))
    if not cand:
        log.warning("No forecast CSV found in data/forecast.")
        return pd.DataFrame(columns=["timestamp","duid","power_hat_MW"])
    f = cand[-1]
    log.info(f"Using forecast CSV: {f.name}")
    return pd.read_csv(f, parse_dates=["timestamp"])

def _latest_day_from_analysis(rep: Dict[str, Any]) -> str:
    if not rep:
        return None
    # any duid entry contains 'day'
    return next(iter(rep.values())).get("day")

def already_done(day: str) -> Path | None:
    if not day:
        return None
    out = Path(f"data/reports/ai_status_{day}.txt")
    return out if _exists(out) else None

def _hourly_means_24(fore: pd.DataFrame) -> Dict[str, Dict[int, float]]:
    if fore.empty:
        return {}
    hf = fore.assign(hour=fore["timestamp"].dt.hour).groupby(["duid","hour"])["power_hat_MW"].mean()
    out: Dict[str, Dict[int, float]] = {}
    for (duid, hour), mw in hf.items():
        out.setdefault(duid, {})[int(hour)] = float(round(mw, 1))
    return out

def build_compact_prompt(rep: Dict[str, Any], fore: pd.DataFrame) -> str:
    lines = ["# KPIs and anomalies summary (compact)"]
    for d, v in sorted(rep.items()):
        lines.append(
            f"{d}|anoms={int(v.get('anomalies',0))}|ramp95={float(v.get('ramp_95p',0)):.1f}|"
            f"trend={float(v.get('slope_mw_per_hr',0)):+.1f}|energy={float(v.get('energy_mwh',0)):.1f}MWh"
        )
    # 24 points per DUID from forecast (hourly mean)
    hm = _hourly_means_24(fore)
    if hm:
        lines.append("\n# Forecast hourly mean MW per DUID")
        for d in sorted(hm.keys()):
            hours = hm[d]
            series = " ".join([f"h{h:02d}={hours.get(h, np.nan):.1f}" for h in range(24)])
            lines.append(f"{d} {series}")
    return "\n".join(lines)

def _estimate_tokens() -> int:
    # conservative fixed estimate to enforce daily cap
    return 2000

def _check_budget_guardrails():
    month = dt.date.today().strftime("%Y-%m")
    cnt = len(list(Path("data/reports").glob(f"ai_status_{month}-*.txt")))
    est_cost = cnt * (0.00015 * 2000)  # gpt-4o-mini ~ $0.15 / 1M tok (ballpark)
    if est_cost > MAX_BUDGET_USD_PER_MONTH:
        raise RuntimeError(f"Budget cap exceeded: est ${est_cost:.3f} > ${MAX_BUDGET_USD_PER_MONTH}.")

def _rule_based_message(rep: Dict[str, Any]) -> str:
    if not rep:
        return "No analysis available yet."
    # decide severity
    critical = False
    notes = []
    for d, v in sorted(rep.items()):
        an = int(v.get("anomalies", 0))
        ramp = float(v.get("ramp_max", 0.0))
        zero_frac = float(v.get("zero_frac", 0.0))
        if an > 3 or zero_frac > 0.2 or ramp > 30:
            critical = True
    if critical:
        # brief callout list
        for d, v in sorted(rep.items()):
            an = int(v.get("anomalies", 0))
            ramp = float(v.get("ramp_max", 0.0))
            if an > 0 or ramp > 0:
                notes.append(f"{d}: anomalies={an}, ramp_max={ramp:.1f} MW/5min")
        return "⚠️ Elevated risk. " + "; ".join(notes[:5])
    return "All systems nominal."

def call_llm(prompt: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        log.warning("OPENAI_API_KEY not set. Falling back to rule-based message.")
        return None  # signal to use fallback

    # Lazy import only if key exists
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    _check_budget_guardrails()
    if _estimate_tokens() > MAX_TOKENS_PER_DAY:
        raise RuntimeError("Token limit exceeded for today.")

    log.info(f"Calling LLM: {MODEL_NAME}")
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": "You are a senior power plant O&M engineer. Be concise, decisive, factual."},
            {"role": "user", "content": f"{prompt}\n\nProduce one headline and 2–4 sentences."}
        ],
        max_tokens=350,
        temperature=0.1,
    )
    return resp.choices[0].message.content

def main():
    log.info("Starting AI Operator (ReAct) agent...")
    rep = load_latest_analysis()
    fore = load_latest_forecast()
    if not rep:
        log.error("No analysis JSON → abort.")
        return

    day = _latest_day_from_analysis(rep)
    if not day:
        log.error("Could not infer 'day' from analysis JSON → abort.")
        return

    out = Path(f"data/reports/ai_status_{day}.txt")
    if _exists(out):
        log.info(f"Cache hit: {out.name} already exists. Not calling LLM again.")
        return

    prompt = build_compact_prompt(rep, fore)
    llm_text = None
    try:
        llm_text = call_llm(prompt)
    except Exception as e:
        log.error(f"LLM call failed: {e}. Falling back to rule-based message.")

    if not llm_text:
        # Fallback ensures we always write a status
        llm_text = _rule_based_message(rep)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(llm_text.strip() + "\n", encoding="utf-8")
    log.info(f"Wrote AI status: {out}")

if __name__ == "__main__":
    main()
