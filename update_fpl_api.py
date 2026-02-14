#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
if not hasattr(np, "float"):
    np.float = float  # temporary patch

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timezone
import json

# Output folders (GitHub-friendly)
STAGE_DIR = Path("data_stage")
PUBLIC_DIR = Path("public")
STAGE_DIR.mkdir(exist_ok=True)
PUBLIC_DIR.mkdir(exist_ok=True)

# Scrape
data = requests.get(
    "https://fantasy.premierleague.com/api/bootstrap-static/",
    timeout=30
).json()

df = pd.json_normalize(data["elements"])

# Store ALL columns (like your original)
df.to_csv(STAGE_DIR / "fpl_api_elements.csv", index=False)

# Website-friendly copy (still ALL columns, just JSON)
df.to_json(PUBLIC_DIR / "fpl_api_elements.json", orient="records")

# Meta (for website "last updated")
meta = {
    "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "row_count": int(len(df)),
    "source": "https://fantasy.premierleague.com/api/bootstrap-static/"
}
(PUBLIC_DIR / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

print("Done. Rows:", len(df))

