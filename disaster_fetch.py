#!/usr/bin/env python3
"""
Chunked, polite GDELT county–date–disaster crawler (location: operator, no pandas).

Output (NDJSON per line):
  {"state": "...", "county": "...", "date": "YYYY-MM-DD", "disaster": "...",
   "articles": [{"title": "...", "url": "...", "source": "..."}]}

Changes vs prior version:
- Uses GDELT location operator: location:"<County>, <State>, US"
- Polite but faster: ~12 RPM (1 request/5s) with exponential backoff + jitter
- Removes pandas; uses csv + defaultdict for grouping
- Keeps absolute COUNTIES_CSV path as provided
- Chunked processing + cooldown; resume via CSV log
"""

import csv
import json
import random
import re
import signal
import time
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- paths (keep your absolute counties path) ----------------
COUNTIES_CSV = "/Users/macintosh-computadora/P2 472/GDELT-project-/County Fetching/us_counties.csv"
OUT_NDJSON   = "gdelt_county_disasters.ndjson"
LOG_CSV      = "gdelt_query_log.csv"

# ---------------- knobs ----------------
DISASTERS = ["hurricane", "flood", "tornado", "wildfire", "earthquake", "drought", "storm"]

MAX_RECORDS        = 250          # API max
RATE_LIMIT_RPM     = 8         # ~1 request every 5 seconds
RETRIES            = 8
TIMEOUT            = 30
SLEEP_BASE         = 1.5
BACKOFF_MAX        = 120.0
QUERY_SCOPE        = "sourcecountry:US timespan:180d"  # start with 6 months; widen later if needed

CHUNK_SIZE         = 80           # keys per chunk
CHUNK_COOLDOWN_SEC = 180          # 3 min between chunks (polite)
MAX_CHUNKS_PER_RUN = None         # e.g., 10 to cap a run; None = all

SLEEP_BETWEEN_KEYS = 0.0          # pacing handled by global limiter
RANDOMIZE_KEYS     = True         # spread load across geography/disasters

USER_AGENT         = "Utkarsh-GDELT-Research-Crawler/2.0 (+local use)"

# ---------------- rate limiter ----------------
_min_interval = 60.0 / max(RATE_LIMIT_RPM, 1)
_last_request_ts = 0.0

def rate_limit_sleep():
    global _last_request_ts
    now = time.monotonic()
    wait = _min_interval - (now - _last_request_ts)
    # small jitter to avoid lockstep patterns
    wait = max(0.0, wait) + random.uniform(0.05, 0.25)
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()

# ---------------- HTTP session w/ retries for 5xx ----------------
def make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=3, read=3, connect=3,
        backoff_factor=1.0,
        status_forcelist=(502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess

SESSION = make_session()

# ---------------- utils ----------------
def chunked(iterable: Iterable, size: int):
    """Yield items in fixed-size chunks."""
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def county_query_string(county_name_clean: str, state_name: str) -> str:
    """
    Use GDELT location operator to filter on structured geocoding.
    Example: location:"Maricopa County, Arizona, US"
    """
    return f'location:"{county_name_clean}, {state_name}, US"'

def gdelt_url(query: str) -> str:
    # Include scope (US, time window) in the query
    q = f"{query} {QUERY_SCOPE}".strip()
    return (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(q)}&mode=artlist&maxrecords={MAX_RECORDS}&format=csv&sort=datedesc"
    )

def normalize_date(val) -> str:
    """
    GDELT 'Date' is YYYYMMDDHHMMSS. Keep YYYY-MM-DD.
    """
    if val is None:
        return ""
    s = str(val)
    m = re.match(r"^(\d{8})", s)
    if not m:
        return ""
    ymd = m.group(1)
    return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

# ---------------- parsing & grouping (no pandas) ----------------
def rows_from_csv_text(csv_text: str, state: str, county: str, disaster: str) -> List[Dict]:
    """
    Parse the DOC API CSV text and return grouped rows:
    [{state, county, date, disaster, articles:[{title,url,source}, ...]}, ...]
    """
    if not csv_text or "DocumentIdentifier" not in csv_text:
        return []

    reader = csv.DictReader(StringIO(csv_text))
    seen_urls = set()
    by_date: Dict[str, List[Dict]] = defaultdict(list)

    for row in reader:
        url = row.get("DocumentIdentifier") or ""
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        date_str = normalize_date(row.get("Date"))
        if not date_str:
            continue

        title = row.get("Title") or ""
        source = row.get("SourceCommonName") or ""
        # quick hostname fallback if SourceCommonName is empty
        if not source:
            m = re.search(r"https?://([^/]+)", url)
            if m:
                source = m.group(1)

        by_date[date_str].append({"title": title, "url": url, "source": source})

    out = []
    for dte, arts in by_date.items():
        if arts:
            out.append({
                "state": state,
                "county": county,
                "date": dte,
                "disaster": disaster,
                "articles": arts,
            })

    # keep newest first (matches sort=datedesc)
    out.sort(key=lambda r: r["date"], reverse=True)
    return out

# ---------------- file I/O helpers ----------------
def append_ndjson(path: str, records: List[Dict]) -> None:
    if not records:
        return
    with open(path, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def log_progress(path: str, state: str, county: str, disaster: str, n_dates: int, n_articles: int) -> None:
    p = Path(path)
    need_header = not p.exists() or p.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if need_header:
            w.writerow(["state", "county", "disaster", "date_groups", "articles"])
        w.writerow([state, county, disaster, n_dates, n_articles])

def load_done_keys(log_csv: str) -> set:
    """
    Read already-processed (state, county, disaster) triplets from a tolerant CSV.
    """
    done = set()
    p = Path(log_csv)
    if not p.exists() or p.stat().st_size == 0:
        return done
    with open(log_csv, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if not rd.fieldnames:
            return done
        # strip whitespace & lowercase header names
        lower = {h.strip().lower(): h for h in rd.fieldnames}
        s, c, d = lower.get("state"), lower.get("county"), lower.get("disaster")
        if not (s and c and d):
            return done
        for row in rd:
            st = (row.get(s, "") or "").strip()
            co = (row.get(c, "") or "").strip()
            di = (row.get(d, "") or "").strip()
            if st and co and di:
                done.add((st, co, di))
    return done

def load_counties(path: str) -> List[Tuple[str, str]]:
    """
    Expect columns: state_name, county_name_clean
    """
    rows: List[Tuple[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        # tolerate header variants
        lower = {h.strip().lower(): h for h in (rd.fieldnames or [])}
        s_col = lower.get("state_name") or lower.get("state")
        c_col = lower.get("county_name_clean") or lower.get("countyclean") or lower.get("county")
        if not (s_col and c_col):
            raise SystemExit("us_counties.csv must include 'state_name' and 'county_name_clean' columns.")
        for r in rd:
            s = (r.get(s_col, "") or "").strip()
            c = (r.get(c_col, "") or "").strip()
            if s and c:
                rows.append((s, c))
    return rows

# ---------------- fetch with 429 handling ----------------
def fetch_gdelt(query: str) -> str:
    """
    Return the raw CSV string from the DOC API for a given query.
    """
    url = gdelt_url(query)
    backoff = SLEEP_BASE
    last_err = None
    streak_429 = 0

    for attempt in range(1, RETRIES + 1):
        rate_limit_sleep()
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            txt = r.text or ""

            if r.status_code == 200 and "DocumentIdentifier" in txt and len(txt) > 100:
                return txt

            if r.status_code == 429:
                streak_429 += 1
                ra = r.headers.get("Retry-After")
                try:
                    sleep_s = float(ra) if ra else backoff + random.uniform(0, 0.5)
                except Exception:
                    sleep_s = backoff + random.uniform(0, 0.5)
                print(f"[rate] 429 attempt {attempt}. sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX)
                if streak_429 >= 3:
                    cool = 180 + random.uniform(0, 60)  # extra cool-down
                    print(f"[rate] 429 streak={streak_429}. cooling {cool:.1f}s")
                    time.sleep(cool)
                    streak_429 = 0
                continue

            if r.status_code in (502, 503, 504):
                sleep_s = backoff + random.uniform(0, 0.5)
                print(f"[rate] {r.status_code} transient. sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue

            last_err = f"status {r.status_code}, len={len(txt)}"
        except Exception as e:
            last_err = str(e)

        # generic retry
        sleep_s = backoff + random.uniform(0, 0.5)
        time.sleep(sleep_s)
        backoff = min(backoff * 2, BACKOFF_MAX)

    print(f"[warn] query failed after {RETRIES} tries. reason={last_err} q={query[:120]}")
    return ""

# ---------------- graceful Ctrl-C ----------------
_running = True
def _sigint_handler(sig, frame):
    global _running
    _running = False
    print("\n[info] Ctrl-C received. Finishing current chunk politely…")
signal.signal(signal.SIGINT, _sigint_handler)

# ---------------- main (chunked) ----------------
def main():
    if not Path(COUNTIES_CSV).exists():
        raise SystemExit(f"counties file not found at {COUNTIES_CSV}")

    counties = load_counties(COUNTIES_CSV)
    print(f"loaded {len(counties)} counties from {COUNTIES_CSV}")

    # build all keys
    all_keys = [(s, c, d) for (s, c) in counties for d in DISASTERS]

    # resume
    done = load_done_keys(LOG_CSV)
    pending = [k for k in all_keys if k not in done]
    if RANDOMIZE_KEYS:
        random.shuffle(pending)

    print(f"pending keys to process: {len(pending)}")

    chunks_iter = list(chunked(pending, CHUNK_SIZE))
    if MAX_CHUNKS_PER_RUN is not None:
        chunks_iter = chunks_iter[:MAX_CHUNKS_PER_RUN]

    for idx, ch in enumerate(chunks_iter, 1):
        if not _running:
            break
        print(f"[chunk {idx}/{len(chunks_iter)}] processing {len(ch)} keys…")

        for (state, county_clean, disaster) in ch:
            if not _running:
                break

            # location filter + disaster keyword (accurate targeting)
            loc_query = county_query_string(county_clean, state)
            q = f"{disaster} {loc_query}"

            csv_text = fetch_gdelt(q)
            entries = rows_from_csv_text(csv_text, state=state, county=county_clean, disaster=disaster)

            append_ndjson(OUT_NDJSON, entries)
            n_dates = len(entries)
            n_articles = sum(len(e["articles"]) for e in entries)
            log_progress(LOG_CSV, state, county_clean, disaster, n_dates, n_articles)

            if SLEEP_BETWEEN_KEYS > 0:
                time.sleep(SLEEP_BETWEEN_KEYS)

        if idx < len(chunks_iter) and _running:
            print(f"[chunk {idx}] cool-down for {CHUNK_COOLDOWN_SEC}s…")
            time.sleep(CHUNK_COOLDOWN_SEC)

    print("done. dataset appended to", OUT_NDJSON)
    print("progress logged to", LOG_CSV)

if __name__ == "__main__":
    main()
