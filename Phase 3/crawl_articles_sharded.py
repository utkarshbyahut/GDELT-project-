#!/usr/bin/env python3
"""
Phase 4 – Sharded crawler with multiple workers.

- Input:
    articles_with_id.ndjson  (one JSON per line, includes unique_entity_identifier, url, etc.)

- Existing global log (optional, for resume):
    crawl_raw.ndjson         (old single-file crawl log; used ONLY to detect already-done IDs)

- Output:
    crawl_shards/out01.ndjson ... out20.ndjson
    Each file contains NDJSON lines with full article crawl + metadata,
    keyed by unique_entity_identifier.

- Behavior:
    * Reads articles_with_id.ndjson
    * Loads IDs already present in:
        - crawl_raw.ndjson                 (if exists)
        - crawl_shards/out*.ndjson         (if exist)
    * Skips any IDs already crawled
    * Uses 20 worker threads to fetch & extract article content
    * Shards each result to one of 20 files based on hash(unique_entity_identifier) % 20

Usage:
    python crawl_articles_sharded.py
    python crawl_articles_sharded.py --input articles_with_id.ndjson \
                                     --global-log crawl_raw.ndjson \
                                     --shard-dir crawl_shards \
                                     --workers 20 --shards 20
"""

import argparse
import json
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List

import re
from collections import Counter
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    # Optional but recommended for better article extraction
    import trafilatura
except ImportError:
    trafilatura = None


# ------------- Config defaults -------------

DEFAULT_INPUT = "articles_with_id.ndjson"
DEFAULT_GLOBAL_LOG = "crawl_raw.ndjson"      # old monolithic output (if any)
DEFAULT_SHARD_DIR = "crawl_shards"
DEFAULT_WORKERS = 20
DEFAULT_SHARDS = 20

REQUEST_TIMEOUT = 15
REQUEST_SLEEP = 0.2  # small politeness delay in worker, can tweak
HEADERS = {
    "User-Agent": "UtkarshDisasterCrawler/1.0 (+for research; contact: your-email@example.com)"
}


# ------------- Utility helpers -------------

def now_utc_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def print_progress(prefix: str, done: int, total: int):
    bar_len = 40
    if total <= 0:
        total = 1
    frac = done / total
    filled = int(bar_len * frac)
    bar = "#" * filled + "-" * (bar_len - filled)
    sys.stdout.write(f"\r{prefix} [{bar}] {done}/{total}")
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n")


def tokenize(text: str):
    """Simple word tokenizer: lowercase, alphanumeric tokens."""
    return re.findall(r"\b\w+\b", text.lower())


def load_seen_ids_from_file(path: Path) -> set:
    """Collect unique_entity_identifier values from a single NDJSON file, if it exists."""
    seen = set()
    if not path.exists():
        return seen

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            uid = obj.get("unique_entity_identifier")
            if uid:
                seen.add(uid)
    return seen


def load_seen_ids_from_dir(dir_path: Path) -> set:
    """Collect unique_entity_identifier from all *.ndjson files in a directory."""
    seen = set()
    if not dir_path.exists():
        return seen

    for ndj in dir_path.glob("*.ndjson"):
        seen |= load_seen_ids_from_file(ndj)
    return seen


# ------------- Fetch + extraction helpers -------------

def extract_with_trafilatura(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    If trafilatura is available, use it to fetch and extract.
    Returns (text, title) or (None, None) on failure.
    """
    if trafilatura is None:
        return None, None

    downloaded = trafilatura.fetch_url(url, timeout=REQUEST_TIMEOUT)
    if not downloaded:
        return None, None

    extracted_json = trafilatura.extract(
        downloaded,
        output_format="json",
        with_metadata=True,
        include_comments=False,
        include_tables=False,
    )
    if not extracted_json:
        return None, None

    try:
        data = json.loads(extracted_json)
    except Exception:
        return None, None

    text = (data.get("text") or "").strip()
    title = (data.get("title") or "").strip() or None
    if not text:
        return None, title
    return text, title


def fetch_and_extract(url: str) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch URL and extract article text and metadata.

    Returns:
        (article_text, meta_dict)
        article_text may be "" on failure.
        meta_dict contains: fetch_status, http_status, error_reason, final_url, article_title
    """
    meta: Dict[str, Any] = {
        "fetch_status": None,
        "http_status": None,
        "error_reason": None,
        "final_url": None,
        "article_title": None,
    }

    # Try trafilatura end-to-end if available
    if trafilatura is not None:
        try:
            text, title = extract_with_trafilatura(url)
            if text:
                meta["fetch_status"] = "ok"
                meta["http_status"] = 200  # trafilatura abstracts HTTP details
                meta["final_url"] = url
                meta["article_title"] = title
                return text, meta
        except Exception as e:
            meta["fetch_status"] = "error"
            meta["error_reason"] = f"trafilatura_error: {e}"

    # Fallback: raw requests + naive text extraction
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        meta["http_status"] = resp.status_code
        meta["final_url"] = str(resp.url)

        if resp.status_code != 200:
            meta["fetch_status"] = "http_error"
            meta["error_reason"] = f"HTTP {resp.status_code}"
            return "", meta

        html = resp.text

        # Remove script/style content
        html_no_scripts = re.sub(
            r"(?is)<(script|style).*?>.*?(</\1>)", "", html
        )
        # Remove all tags
        text = re.sub(r"(?s)<.*?>", " ", html_no_scripts)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        meta["fetch_status"] = "ok" if text else "parse_error"
        if not text:
            meta["error_reason"] = "empty_text_after_naive_extraction"

        return text, meta

    except requests.exceptions.Timeout:
        meta["fetch_status"] = "timeout"
        meta["error_reason"] = "timeout"
        return "", meta
    except Exception as e:
        meta["fetch_status"] = "error"
        meta["error_reason"] = f"exception: {e}"
        return "", meta


# ------------- Job preparation -------------

def load_jobs(input_path: Path, seen_ids: set) -> List[Dict[str, Any]]:
    """
    Load rows from input NDJSON that still need crawling
    (i.e., not in seen_ids and have uid+url).
    """
    jobs = []
    total = 0

    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue

            total += 1
            uid = row.get("unique_entity_identifier")
            url = row.get("url")
            if not uid or not url:
                continue
            if uid in seen_ids:
                continue

            jobs.append(row)

    print(f"[info] Input total rows: ~{total}")
    print(f"[info] Already crawled IDs: {len(seen_ids)}")
    print(f"[info] Rows remaining to crawl: {len(jobs)}")
    return jobs


# ------------- Worker function -------------

def process_row(row: Dict[str, Any], num_shards: int) -> Tuple[str, int, Dict[str, Any]]:
    """
    Worker function: fetch, extract, build out_obj, decide shard index.

    Returns:
        (unique_entity_identifier, shard_idx, out_obj)
    """
    uid = row["unique_entity_identifier"]
    url = row["url"]

    article_text, meta = fetch_and_extract(url)

    tokens = tokenize(article_text) if article_text else []
    word_freq = dict(Counter(tokens))

    out_obj: Dict[str, Any] = {
        "unique_entity_identifier": uid,
        "url": url,
        "domain": row.get("domain"),
        "sourceCountry": row.get("sourceCountry"),
        "disasterNumber": row.get("disasterNumber"),
        "county_fips": row.get("county_fips"),
        "state": row.get("state"),
        "county": row.get("county"),
        "incidentType": row.get("incidentType"),
        "window_start": row.get("window_start"),
        "window_end": row.get("window_end"),
        "gdelt_datetime": row.get("datetime"),
        "gdelt_title": row.get("title"),
        "gdelt_lang": row.get("lang"),
        "gdelt_score": row.get("score"),
        "gdelt_label": row.get("label"),
        "gdelt_query": row.get("gdelt_query"),

        "fetch_status": meta.get("fetch_status"),
        "http_status": meta.get("http_status"),
        "error_reason": meta.get("error_reason"),
        "final_url": meta.get("final_url"),
        "article_title": meta.get("article_title") or row.get("title"),

        "article_text": article_text,
        "article_word_count": len(tokens),
        "article_char_count": len(article_text),
        "article_language": row.get("lang") or None,

        "word_list": tokens,
        "word_freq": word_freq,

        "crawl_timestamp_utc": now_utc_iso(),
    }

    # Decide shard: deterministic based on uid
    shard_idx = hash(uid) % num_shards

    # politeness delay per worker
    time.sleep(REQUEST_SLEEP)

    return uid, shard_idx, out_obj


# ------------- Main sharded crawl -------------

def crawl_sharded(
    input_path: Path,
    global_log_path: Path,
    shard_dir: Path,
    num_workers: int,
    num_shards: int,
):
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    shard_dir.mkdir(parents=True, exist_ok=True)

    # Collect already-done IDs from global log + shard files (resume support)
    seen_ids = set()
    seen_ids |= load_seen_ids_from_file(global_log_path)
    seen_ids |= load_seen_ids_from_dir(shard_dir)

    jobs = load_jobs(input_path, seen_ids)
    total_jobs = len(jobs)
    if total_jobs == 0:
        print("[info] Nothing to crawl – all IDs already processed.")
        return

    # Open shard files
    shard_files = []
    for i in range(num_shards):
        shard_name = f"out{i+1:02d}.ndjson"  # out01, out02, ...
        shard_path = shard_dir / shard_name
        shard_f = shard_path.open("a", encoding="utf-8")
        shard_files.append(shard_f)

    print(f"[info] Using {num_workers} workers and {num_shards} shards.")
    completed = 0

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(process_row, row, num_shards)
                for row in jobs
            ]

            for fut in as_completed(futures):
                try:
                    uid, shard_idx, out_obj = fut.result()
                except Exception as e:
                    # In case of catastrophic failure in worker, skip this job
                    print(f"\n[warn] Worker error: {e}")
                    completed += 1
                    print_progress("[crawl] Progress", completed, total_jobs)
                    continue

                # Write to appropriate shard
                f = shard_files[shard_idx]
                f.write(json.dumps(out_obj, ensure_ascii=False) + "\n")

                completed += 1
                print_progress("[crawl] Progress", completed, total_jobs)

    finally:
        for f in shard_files:
            f.close()

    print(f"\n[done] Completed {completed}/{total_jobs} new crawls into {shard_dir}")


def main():
    ap = argparse.ArgumentParser(
        description="Phase 4 (sharded): Crawl article pages with multiple workers and write to shard files."
    )
    ap.add_argument("--input", type=str, default=DEFAULT_INPUT,
                    help="Input NDJSON with unique_entity_identifier + url (default: articles_with_id.ndjson)")
    ap.add_argument("--global-log", type=str, default=DEFAULT_GLOBAL_LOG,
                    help="Existing global log (crawl_raw.ndjson) used only to detect already-crawled IDs")
    ap.add_argument("--shard-dir", type=str, default=DEFAULT_SHARD_DIR,
                    help="Directory to store shard NDJSON files (default: crawl_shards)")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help="Number of worker threads (default: 20)")
    ap.add_argument("--shards", type=int, default=DEFAULT_SHARDS,
                    help="Number of shard files (default: 20)")
    args = ap.parse_args()

    crawl_sharded(
        input_path=Path(args.input),
        global_log_path=Path(args.global_log),
        shard_dir=Path(args.shard-dir) if False else Path(args.shard_dir),
        num_workers=args.workers,
        num_shards=args.shards,
    )


if __name__ == "__main__":
    main()
