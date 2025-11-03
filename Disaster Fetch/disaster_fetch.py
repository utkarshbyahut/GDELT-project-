#!/usr/bin/env python3
"""
Chunked, adaptive, polite GDELT county–date–disaster crawler
- Uses location:"<County>, <State>, US" and theme:... filters (accurate + low-noise)
- Global rate limiter with adaptive slow-down on repeated 429s
- Global cool-off across the whole process after any 429
- Chunked processing with cooldowns between chunks
- Resumable via CSV log
- No pandas; lightweight csv parsing

Output (NDJSON per line):
  {"state": "...", "county": "...", "date": "YYYY-MM-DD", "disaster": "...",
   "articles": [{"title": "...", "url": "...", "source": "..."}]}
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

# Map each disaster to one or more GKG theme tokens (used via DOC API theme: operator)
DISASTER_THEMES = {
    "flood":      ["NATURAL_DISASTER_FLOOD", "FLOOD"],
    "wildfire":   ["WILDFIRE", "FIRE_WILDFIRE"],
    "drought":    ["DROUGHT", "ENV_DROUGHT"],
    "earthquake": ["EARTHQUAKE"],
    "hurricane":  ["HURRICANE", "TROPICAL_CYCLONE", "TYPHOON"],
    "tornado":    ["TORNADO"],
    "storm":      ["STORM", "SEVERE_WEATHER"],
}

MAX_RECORDS        = 250
RATE_LIMIT_RPM     = 9            # CHANGED: baseline ~6.7s/req, but see MIN_API_INTERVAL floor below
RETRIES            = 8
TIMEOUT            = 30
SLEEP_BASE         = 1.5
BACKOFF_MAX        = 180.0
QUERY_SCOPE        = "sourcecountry:US timespan:30d"   # CHANGED: start tight; widen in later passes

CHUNK_SIZE         = 20           # CHANGED: smaller bursts
CHUNK_COOLDOWN_SEC = 300          # CHANGED: 5 minutes between chunks
MAX_CHUNKS_PER_RUN = None

SLEEP_BETWEEN_KEYS = 0.0
RANDOMIZE_KEYS     = True

# ---- hard minimum per API guidance (≥ 1 request every ~5 seconds) ----
MIN_API_INTERVAL   = 5.5          # CHANGED: floor gap between requests

# Startup warm-up to avoid rolling-window throttles after restarts
STARTUP_WARMUP_SEC = 600          # CHANGED: 10 minutes before first call

# Randomized UA each run to avoid exact fingerprinting
BASE_USER_AGENT    = "Utkarsh-GDELT-Research-Crawler/3.1"

# ---------------- global rate/429 state ----------------
_min_interval = 60.0 / max(RATE_LIMIT_RPM, 1)
_last_request_ts = 0.0

_last_429_ts = 0.0
GLOBAL_429_COOLDOWN = 300.0

_adaptive_factor = 1.0
ADAPTIVE_MAX_FACTOR = 4.0
ADAPTIVE_DECAY_SEC = 600.0
_last_adapt_ts = 0.0

def rate_limit_sleep():
    """Global limiter: enforces MIN_API_INTERVAL floor + adaptive slow-down."""
    global _last_request_ts, _adaptive_factor, _last_adapt_ts
    # decay adaptive factor
    if _adaptive_factor > 1.0 and (time.monotonic() - _last_adapt_ts) > ADAPTIVE_DECAY_SEC:
        _adaptive_factor = max(1.0, _adaptive_factor * 0.8)
        _last_adapt_ts = time.monotonic()

    now = time.monotonic()
    base_interval = 60.0 / max(RATE_LIMIT_RPM, 1)
    interval = max(MIN_API_INTERVAL, base_interval) * _adaptive_factor  # CHANGED: enforce floor
    wait = interval - (now - _last_request_ts)
    wait = max(0.0, wait) + random.uniform(0.05, 0.25)                  # small jitter
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
    # CHANGED: randomized UA per run
    sess.headers.update({"User-Agent": f"{BASE_USER_AGENT} ({random.randint(1000,9999)})"})
    return sess

SESSION = make_session()

# ---------------- utils ----------------
def chunked(iterable: Iterable, size: int):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def disaster_theme_clause(disaster: str) -> str:
    themes = DISASTER_THEMES.get(disaster, [])
    if not themes:
        return disaster
    ors = " OR ".join(f'theme:{t}' for t in themes)
    return f"({ors})"

def county_query_string(county_name_clean: str, state_name: str) -> str:
    """
    Use GDELT location operator; add common suffixes when missing.
    """
    c = county_name_clean.strip()
    lc = c.lower()
    st_lc = state_name.strip().lower()

    if "parish" in lc:
        label = c
    elif "city and borough" in lc or lc.endswith(" city") or lc == "city":
        label = c
    elif "borough" in lc or "census area" in lc:
        label = c
    elif st_lc in ("district of columbia", "washington, d.c.", "dc", "d.c."):
        label = c
    else:
        label = c if lc.endswith(" county") else f"{c} County"

    return f'location:"{label}, {state_name}, US"'

def gdelt_url(query: str) -> str:
    q = f"{query} {QUERY_SCOPE}".strip()
    return (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote_plus(q)}&mode=artlist&maxrecords={MAX_RECORDS}&format=csv&sort=datedesc"
    )

def normalize_date(val) -> str:
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
    if not csv_text or "DocumentIdentifier" not in csv_text:
        return []

    reader = csv.DictReader(StringIO(csv_text))
    seen_urls = set()
    by_date: Dict[str, List[Dict]] = defaultdict(list)

    for row in reader:
        url = (row.get("DocumentIdentifier") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        date_str = normalize_date(row.get("Date"))
        if not date_str:
            continue

        title = row.get("Title") or ""
        source = row.get("SourceCommonName") or ""
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
    done = set()
    p = Path(log_csv)
    if not p.exists() or p.stat().st_size == 0:
        return done
    with open(log_csv, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if not rd.fieldnames:
            return done
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
    rows: List[Tuple[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
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

# ---------------- fetch with global 429 handling ----------------
def fetch_gdelt(query: str) -> str:
    """
    Return the raw CSV string from the DOC API for a given query.
    Adds:
      - global cool-off if any earlier request saw a 429
      - adaptive throttle bumps on 429
    """
    global _last_429_ts, _adaptive_factor, _last_adapt_ts

    url = gdelt_url(query)
    backoff = SLEEP_BASE
    last_err = None
    streak_429 = 0

    for attempt in range(1, RETRIES + 1):
        # global RPM limiter
        rate_limit_sleep()

        # if we recently saw a 429 anywhere, honor a global cool-off
        since = time.monotonic() - _last_429_ts
        if since < GLOBAL_429_COOLDOWN:
            sleep_s = GLOBAL_429_COOLDOWN - since + random.uniform(0.1, 0.5)
            print(f"[rate] global cool-off active. sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)

        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            txt = r.text or ""

            if r.status_code == 200 and "DocumentIdentifier" in txt and len(txt) > 100:
                time.sleep(2.0)  # CHANGED: tiny success delay to avoid edge hits
                return txt

            if r.status_code == 429:
                _last_429_ts = time.monotonic()
                streak_429 += 1

                _adaptive_factor = min(ADAPTIVE_MAX_FACTOR, _adaptive_factor * 1.25)
                _last_adapt_ts = time.monotonic()

                ra = r.headers.get("Retry-After")
                try:
                    sleep_s = float(ra) if ra else backoff + random.uniform(0, 0.5)
                except Exception:
                    sleep_s = backoff + random.uniform(0, 0.5)
                print(f"[rate] 429 attempt {attempt}. sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX)

                if streak_429 >= 3:
                    cool = 300 + random.uniform(0, 60)
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

    # CHANGED: startup warm-up to avoid rolling-window 429 after restart
    if pending:
        print(f"[start] warm-up sleep {STARTUP_WARMUP_SEC}s to avoid rolling-window 429…")
        time.sleep(STARTUP_WARMUP_SEC)

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

            loc_query = county_query_string(county_clean, state)
            disaster_clause = disaster_theme_clause(disaster)
            q = f"{disaster_clause} {loc_query}"

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
