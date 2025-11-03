#!/usr/bin/env python3
"""
Fetch FEMA Disaster Declarations and build a county–date–disaster NDJSON spine.

Fixes:
- Use correct field names (FEMA is case-sensitive): designatedArea (not declaredCountyArea)
- Use full ISO datetimes in $filter
- Add $format=json
- On 400, print FEMA error body; also retry once without $orderby
"""

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Dict, Tuple, Optional

import requests

# ---- your absolute counties CSV path (kept as requested) ----
COUNTIES_CSV = "/Users/macintosh-computadora/P2 472/GDELT-project-/County Fetching/us_counties.csv"
BASE_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

PAGE_SIZE   = 1000   # FEMA allows up to 10,000; 1,000 is safe & friendly
RETRIES     = 5
TIMEOUT     = 30
SLEEP_BETWEEN = 0.20
BACKOFF_BASE  = 1.5
BACKOFF_MAX   = 20.0

# map FEMA incidentType labels to your normalized disaster names
NORMALIZE = {
    "flood": "flood",
    "flash flood": "flood",
    "severe storm(s)": "storm",
    "severe storm": "storm",
    "severe storms": "storm",
    "hurricane": "hurricane",
    "typhoon": "hurricane",
    "tornado": "tornado",
    "fire": "wildfire",
    "wildfire": "wildfire",
    "earthquake": "earthquake",
    "drought": "drought",
}

def load_fips_mapping(path: str) -> Dict[str, Tuple[str, str]]:
    """
    Build map: 5-digit county FIPS -> (state_name, county_name_clean)
    Accepts either:
      - county_geoid (e.g., 1001 for 01001), or
      - state_fips + county_fips (e.g., 1 + 1 -> 01001)
    """
    m: Dict[str, Tuple[str, str]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        lowers = {h.strip().lower(): h for h in (rd.fieldnames or [])}

        s_name = lowers.get("state_name") or lowers.get("state")
        c_clean = lowers.get("county_name_clean") or lowers.get("countyclean") or lowers.get("county")

        # columns available in your file
        geoid_col = lowers.get("county_geoid")
        s_fips_col = lowers.get("state_fips")
        c_fips_col = lowers.get("county_fips")

        if not (s_name and c_clean and (geoid_col or (s_fips_col and c_fips_col))):
            raise SystemExit(
                "counties file must have state_name, county_name_clean, and either county_geoid "
                "or both state_fips + county_fips"
            )

        for r in rd:
            state = (r.get(s_name, "") or "").strip()
            county = (r.get(c_clean, "") or "").strip()

            fips5 = None
            if geoid_col and r.get(geoid_col) not in (None, ""):
                # county_geoid like 1001 → "01001"
                fips5 = str(r[geoid_col]).strip()
                # tolerate ints
                if fips5.isdigit():
                    fips5 = fips5.zfill(5)
                else:
                    fips5 = re.sub(r"\D", "", fips5).zfill(5)
            elif s_fips_col and c_fips_col:
                s2 = str(r.get(s_fips_col, "") or "").strip()
                c3 = str(r.get(c_fips_col, "") or "").strip()
                s2 = re.sub(r"\D", "", s2).zfill(2)
                c3 = re.sub(r"\D", "", c3).zfill(3)
                if s2 and c3 and s2.isdigit() and c3.isdigit():
                    fips5 = s2 + c3

            if fips5 and state and county:
                m[fips5] = (state, county)

    if not m:
        raise SystemExit("no county FIPS keys were built; check CSV column names/values")
    return m

def norm_date(date_str: str) -> Optional[str]:
    if not date_str:
        return None
    return date_str[:10]  # YYYY-MM-DD from ISO timestamp

def norm_disaster(label: str) -> Optional[str]:
    if not label:
        return None
    key = label.strip().lower()
    if key in NORMALIZE:
        return NORMALIZE[key]
    if "storm" in key: return "storm"
    if "flood" in key: return "flood"
    if "fire" in key or "wildfire" in key: return "wildfire"
    if "hurricane" in key or "typhoon" in key or "cyclone" in key: return "hurricane"
    if "tornado" in key: return "tornado"
    if "earthquake" in key: return "earthquake"
    if "drought" in key: return "drought"
    return "storm"

def build_filter(start_iso: str, end_iso: Optional[str], state_abbr: Optional[str], fema_types: Optional[str]) -> str:
    """
    OData filter. FEMA is strict about spacing, quotes, and case.
    Example produced:
      declarationDate ge '2015-01-01T00:00:00Z' and state eq 'AZ' and (incidentType eq 'Flood' or incidentType eq 'Fire')
    """
    parts = [f"declarationDate ge '{start_iso}'"]
    if end_iso:
        parts.append(f"declarationDate le '{end_iso}'")
    if state_abbr:
        parts.append(f"state eq '{state_abbr.upper()}'")
    if fema_types:
        tys = [t.strip() for t in fema_types.split(",") if t.strip()]
        if tys:
            ors = " or ".join([f"incidentType eq '{t}'" for t in tys])
            parts.append(f"({ors})")
    return " and ".join(parts)

def fetch_page(params: dict) -> dict:
    """
    GET with retries; on 400, print server's error; retry once without $orderby (some gateways reject it).
    """
    backoff = BACKOFF_BASE
    last_err_text = None
    tried_without_order = False

    for _ in range(RETRIES):
        try:
            r = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 400:
                last_err_text = r.text
                if not tried_without_order and "$orderby" in params:
                    p2 = dict(params)
                    p2.pop("$orderby", None)
                    tried_without_order = True
                    r2 = requests.get(BASE_URL, params=p2, timeout=TIMEOUT)
                    if r2.status_code == 200:
                        return r2.json()
                    if r2.status_code == 400:
                        last_err_text = r2.text
                time.sleep(backoff)
                backoff = min(backoff * 1.6, BACKOFF_MAX)
                continue
            if r.status_code in (429, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 1.6, BACKOFF_MAX)
                continue
            r.raise_for_status()
        except Exception as e:
            last_err_text = str(e)
            time.sleep(backoff)
            backoff = min(backoff * 1.6, BACKOFF_MAX)

    if last_err_text:
        print("[warn] fetch_page failed after retries. Server said:\n", last_err_text[:800])
    else:
        print("[warn] fetch_page failed after retries.")
    return {"DisasterDeclarationsSummaries": []}

def write_log_row(log_path: Path, row):
    need_header = not log_path.exists()
    with log_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if need_header:
            w.writerow(["date", "state", "county_fips", "county", "incidentType", "disaster", "disasterNumber"])
        w.writerow(row)

def main():
    ap = argparse.ArgumentParser(description="Fetch FEMA Disaster Declarations into NDJSON.")
    ap.add_argument("--out", type=Path, default=Path("spine_fema.ndjson"), help="Output NDJSON path")
    ap.add_argument("--log", type=Path, default=Path("spine_fema_log.csv"), help="Progress CSV log path")
    ap.add_argument("--start-date", type=str, default="2015-01-01", help="Inclusive start (YYYY-MM-DD)")
    ap.add_argument("--end-date", type=str, default=None, help="Inclusive end (YYYY-MM-DD)")
    ap.add_argument("--state", type=str, default=None, help="Limit to a 2-letter state code (e.g., AZ)")
    ap.add_argument("--fema-types", type=str, default=None, help="Comma list of FEMA incidentType values (e.g., Flood,Hurricane)")
    ap.add_argument("--max-pages", type=int, default=None, help="Stop after N pages (debug)")
    args = ap.parse_args()

    # Convert to full ISO instants for the API filter
    start_iso = args.start_date + "T00:00:00Z"
    end_iso = args.end_date + "T23:59:59Z" if args.end_date else None

    if not Path(COUNTIES_CSV).exists():
        raise SystemExit(f"counties file not found at {COUNTIES_CSV}")

    fips_map = load_fips_mapping(COUNTIES_CSV)

    out_f = args.out.open("a", encoding="utf-8")
    log_path = args.log

    # IMPORTANT: correct, case-sensitive field names
    params = {
        "$select": ",".join([
            "declarationDate",
            "incidentType",
            "designatedArea",     # <-- correct field
            "state",
            "fipsStateCode",
            "fipsCountyCode",
            "disasterNumber",
        ]),
        "$filter": build_filter(start_iso, end_iso, args.state, args.fema_types),
        "$orderby": "declarationDate desc",
        "$top": PAGE_SIZE,
        "$skip": 0,
        "$format": "json",
    }

    print("FEMA fetch started with filter:")
    print(" ", params["$filter"])
    print("Writing NDJSON to:", args.out)
    print("Writing log CSV to:", args.log)

    pages = 0
    rows_written = 0

    while True:
        data = fetch_page(params)
        rows = data.get("DisasterDeclarationsSummaries", [])
        if not rows:
            break

        for d in rows:
            decl_dt = norm_date(d.get("declarationDate") or "")
            if not decl_dt:
                continue

            incident_type = d.get("incidentType") or ""
            normalized = norm_disaster(incident_type)

            # Combine fipsStateCode(2) + fipsCountyCode(3) -> 5-digit county FIPS
            fips2 = (d.get("fipsCountyCode") or "").zfill(3)
            state_num = (d.get("fipsStateCode") or "").zfill(2)
            if not (fips2.isdigit() and state_num.isdigit()):
                continue
            fips5 = state_num + fips2

            if fips5 not in fips_map:
                # Some declarations lack county-level FIPS; skip those
                continue

            state_name, county_clean = fips_map[fips5]
            dis_num = d.get("disasterNumber")

            rec = {
                "state": state_name,
                "county": county_clean,
                "county_fips": fips5,
                "date": decl_dt,
                "disaster": normalized or incident_type.lower(),
                "fema_incident_type": incident_type,
                "fema_disaster_number": dis_num,
                "source": "FEMA",
            }
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            rows_written += 1

            write_log_row(
                log_path,
                [decl_dt, state_name, fips5, county_clean, incident_type, rec["disaster"], dis_num],
            )

        pages += 1
        params["$skip"] += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN)

        if args.max_pages and pages >= args.max_pages:
            print("Stopping early due to --max-pages =", args.max_pages)
            break

    out_f.close()
    print(f"Done. Wrote {rows_written} records across {pages} page(s) to {args.out}")
    print("Log at:", args.log)

if __name__ == "__main__":
    main()
