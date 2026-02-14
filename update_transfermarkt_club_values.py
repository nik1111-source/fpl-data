#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timezone
import json
import re

URL = "https://www.transfermarkt.co.uk/premier-league/marktwerteverein/wettbewerb/GB1"

STAGE_DIR = Path("data_stage")
PUBLIC_DIR = Path("public")
STAGE_DIR.mkdir(exist_ok=True)
PUBLIC_DIR.mkdir(exist_ok=True)

def euro_to_number(s):
    if pd.isna(s):
        return None
    s = str(s).strip()
    if s in {"-", ""}:
        return None
    s = s.replace(",", "")
    m = re.search(r"€\s*([0-9]*\.?[0-9]+)\s*([mb])", s, re.I)
    if not m:
        return None
    value = float(m.group(1))
    unit = m.group(2).lower()
    return value * (1_000_000_000 if unit == "b" else 1_000_000)

# -------------------------
# fetch page (hardened)
# -------------------------
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.transfermarkt.co.uk/",
    "Connection": "keep-alive",
})

resp = session.get(URL, timeout=30)
resp.raise_for_status()
html = resp.text

m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
title = m.group(1).strip()[:120] if m else "NO_TITLE_FOUND"
print(f"[Transfermarkt] HTTP {resp.status_code}, html_len={len(html)}, title='{title}'")

# -------------------------
# parse tables (real block detection happens here)
# -------------------------
tables = pd.read_html(html)  # requires lxml in Actions
if not tables:
    raise RuntimeError("No tables found on page (possible block / layout change).")

# pick table that contains the 'Current value' column if possible
df = None
for t in tables:
    cols = [str(c).strip().lower() for c in t.columns]
    if any("current value" == c for c in cols):
        df = t.copy()
        break

if df is None:
    # fallback: largest table
    df = max(tables, key=lambda x: x.shape[0] * x.shape[1]).copy()

df.columns = [str(c).strip() for c in df.columns]

# Remove footer total row
mask_total = df.apply(
    lambda r: r.astype(str).str.contains("Total value of all clubs", case=False, na=False).any(),
    axis=1
)
df = df.loc[~mask_total].copy()

# Detect columns dynamically
club_col = "Club" if "Club" in df.columns else df.columns[2]

league_col = None
if "Club.1" in df.columns and df["Club.1"].astype(str).str.contains("Premier League", na=False).mean() > 0.5:
    league_col = "Club.1"
elif "League" in df.columns:
    league_col = "League"

value_date_col = next((c for c in df.columns if re.match(r"^Value\s+\d{2}/\d{2}/\d{4}$", c)), None)
current_value_col = next((c for c in df.columns if c.lower() == "current value"), None)
pct_col = next((c for c in df.columns if c.strip() == "%"), None)

needed = [c for c in [club_col, league_col, value_date_col, current_value_col, pct_col] if c]
clean = df[needed].copy()

rename_map = {club_col: "club"}
if league_col: rename_map[league_col] = "league"
if value_date_col: rename_map[value_date_col] = "value_on_date"
if current_value_col: rename_map[current_value_col] = "current_value"
if pct_col: rename_map[pct_col] = "pct_change"
clean = clean.rename(columns=rename_map)

if "current_value" in clean.columns:
    clean["current_value_eur"] = clean["current_value"].apply(euro_to_number)
if "value_on_date" in clean.columns:
    clean["value_on_date_eur"] = clean["value_on_date"].apply(euro_to_number)

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

clean.to_csv(STAGE_DIR / "transfermarkt_club_values_gb1.csv", index=False)
clean.to_json(PUBLIC_DIR / "transfermarkt_club_values.json", orient="records")

meta = {
    "updated_utc": timestamp,
    "source": URL,
    "row_count": int(len(clean)),
    "http_status": int(resp.status_code),
    "page_title": title,
}
(PUBLIC_DIR / "transfermarkt_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

print("Transfermarkt update complete:", len(clean), "clubs")


