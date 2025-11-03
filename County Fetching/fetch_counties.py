#!/usr/bin/env python3
"""
Fetch all US counties (and equivalents) using Census API PEP Vintage 2023 (charv).

Outputs:
- us_counties.csv
- us_counties.json
"""

import requests
import pandas as pd
import re
from pathlib import Path
from time import sleep

# Valid dataset (Vintage 2023):
# https://api.census.gov/data/2023/pep/charv.html
CENSUS_STATE_URL = "https://api.census.gov/data/2023/pep/charv?get=NAME&for=state:*"
CENSUS_COUNTY_URL_TMPL = "https://api.census.gov/data/2023/pep/charv?get=NAME&for=county:*&in=state:{state_fips}"

OUT_CSV = "us_counties.csv"
OUT_JSON = "us_counties.json"

REMOVALS = [
    r"\bCounty\b",
    r"\bParish\b",
    r"\bBorough\b",
    r"\bCensus Area\b",
    r"\bMunicipality\b",
    r"\bCity and Borough\b",
    r"\bCity\b",
    r"\bMunicipio\b",
    r"\bDistrict\b",
]

def fetch_json(url, timeout=45, retries=3, backoff=1.5):
    for i in range(retries):
        r = requests.get(url, timeout=timeout)
        if r.ok:
            return r.json()
        sleep(backoff * (i + 1))
    r.raise_for_status()

def fetch_states():
    data = fetch_json(CENSUS_STATE_URL)
    header, rows = data[0], data[1:]
    # header: ["NAME", "state"]
    states = [{"state_name": row[0], "state_fips": row[1].zfill(2)} for row in rows]
    return states

def fetch_counties_for_state(state_fips):
    url = CENSUS_COUNTY_URL_TMPL.format(state_fips=state_fips)
    data = fetch_json(url)
    header, rows = data[0], data[1:]
    # header example: ["NAME","state","county"]
    out = []
    for row in rows:
        name, st, county = row[0], row[1].zfill(2), row[2].zfill(3)
        out.append({"state_fips": st, "county_fips": county, "full_label": name})
    return out

def clean_county_name(full_label):
    # e.g., "Miami-Dade County, Florida" -> "Miami-Dade"
    county_part = full_label.split(",")[0].strip()
    cleaned = county_part
    for pat in REMOVALS:
        cleaned = re.sub(pat, "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned

def main():
    print("Fetching states from Census…")
    states = fetch_states()
    print(f"Found {len(states)} states and equivalents")

    all_rows = []
    for s in states:
        st_fips = s["state_fips"]
        st_name = s["state_name"]
        try:
            counties = fetch_counties_for_state(st_fips)
        except Exception as e:
            print(f"Warning: failed counties for {st_name} ({st_fips}): {e}")
            continue

        for c in counties:
            row = {
                "state_name": st_name,
                "state_fips": st_fips,
                "county_name": c["full_label"].split(",")[0].strip(),
                "county_fips": c["county_fips"],
                "county_geoid": f"{st_fips}{c['county_fips']}",
                "county_name_clean": clean_county_name(c["full_label"]),
            }
            all_rows.append(row)

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["county_geoid"]).sort_values(
        ["state_fips", "county_fips"]
    ).reset_index(drop=True)

    Path(OUT_CSV).write_bytes(df.to_csv(index=False).encode("utf-8"))
    Path(OUT_JSON).write_bytes(df.to_json(orient="records", indent=2).encode("utf-8"))

    print(f"Saved {len(df)} counties to {OUT_CSV} and {OUT_JSON}")

if __name__ == "__main__":
    main()
