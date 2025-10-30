# src/agent_react.py
from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path
from openai import OpenAI
import pandas as pd
import numpy as np

# Token/budget limit
MAX_TOKENS_PER_DAY = 5000
MAX_BUDGET_USD_PER_MONTH = 1.0

def load_latest_analysis() -> dict:
    reports = sorted(Path("data/reports").glob("report_*.json"))
    if not reports:
        return {}
    with open(reports[-1], "r", encoding="utf-8") as fp:
        return json.load(fp)

def load_latest_forecast() -> pd.DataFrame:
    cand = sorted(Path("data/forecast").glob("forecast_*_nextday.csv"))
    if not cand:
        return pd.DataFrame(columns=["timestamp","duid","power_hat_MW"])
    return pd.read_csv(cand[-1], parse_dates=["timestamp"])

def build_compact_prompt(rep: dict, fore: pd.DataFrame) -> str:
    lines = ["# KPIs and anomalies summary"]
    for d, v in rep.items():
        lines.append(
            f"{d} | anomalies={v.get('anomalies',0)}, ramp95={v.get('ramp_95p',0):.1f}, trend={v.get('slope_mw_per_hr',0):+.1f} MW/h"
        )

    if not fore.empty:
        hf = fore.assign(hour=fore["timestamp"].dt.hour).groupby(
            ["duid","hour"]
        )["power_hat_MW"].mean().round(1)
        
        lines.append("\n# Forecast hourly mean MW")
        for (d,h),mw in hf.items():
            lines.append(f"{d} h{h:02d}={mw:.1f}")

    return "\n".join(lines)

def already_done(day: str) -> bool:
    f = Path(f"data/reports/ai_status_{day}.txt")
    return f.exists()

def estimate_cost_tokens(resp):
    # Approx local estimate: input + output tokens
    # We set a fixed conservative estimate to avoid undercounting
    return 2000  # ~5k/day limit

def check_budget_guardrails():
    # PROTECT AGAINST OVERSPEND
    # → simple guard: if too many daily runs exist in month
    month = dt.date.today().strftime("%Y-%m")
    count = len(list(Path("data/reports").glob(f"ai_status_{month}-*.txt")))
    est_cost = count * (0.00015 * 2000)  # gpt-4o-mini pricing approx
    if est_cost > MAX_BUDGET_USD_PER_MONTH:
        raise RuntimeError(f"Budget cap exceeded: est ${est_cost:.3f} > ${MAX_BUDGET_USD_PER_MONTH}.")

def call_llm(prompt: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "ERROR: No OpenAI API key set."
    client = OpenAI(api_key=api_key)

    # Guardrails
    check_budget_guardrails()

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a senior power plant O&M engineer."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=350,  # ~1 short paragraph
        temperature=0.1,
    )

    estimated = estimate_cost_tokens(resp)
    if estimated > MAX_TOKENS_PER_DAY:
        raise RuntimeError("Token limit exceeded for today.")

    return resp.choices[0].message.content

def main():
    rep = load_latest_analysis()
    fore = load_latest_forecast()
    if not rep:
        print("❌ No analysis found for today.")
        return

    # DAY IDENTIFICATION
    day = list(rep.values())[0]["day"]
    out = Path(f"data/reports/ai_status_{day}.txt")

    # CACHE: if already done today → reuse
    if out.exists():
        print(f"✅ Using cached AI status: {out}")
        return

    prompt = build_compact_prompt(rep, fore)
    msg = call_llm(prompt)

    out.write_text(msg, encoding="utf-8")
    print(f"✅ New AI operator status generated for {day}\n{msg}")
