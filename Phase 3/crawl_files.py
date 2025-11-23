#!/usr/bin/env python3
"""
Phase 4 – Crawl article pages and store text + metadata keyed by unique_entity_identifier.

Input:
    articles_with_id.ndjson   (one JSON per line, includes unique_entity_identifier, url, etc.)

Output:
    crawl_raw.ndjson          (one JSON per line, with crawl + article data)
    - Only NEW unique_entity_identifier values are crawled if output already exists.

Usage:
    python crawl_articles.py
    python crawl_articles.py --input Phase3/Backup\ with\ ID/articles_with_id.ndjson \
                             --output crawl_raw.ndjson
"""

import argparse
import json
import sys
import time
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, Optional

import requests

try:
    # Optional but recommended for better article extraction
    import trafilatura
except ImportError:
    trafilatura = None

from collections import Counter
import re


# ------------- Config -------------

DEFAULT_INPUT = "articles_with_id.ndjson"
DEFAULT_OUTPUT = "crawl_raw.ndjson"

REQUEST_TIMEOUT = 15
REQUEST_SLEEP = 0.5  # polite delay between requests
HEADERS = {
    "User-Agent": "UtkarshDisasterCrawler/1.0 (+for research; contact: your-email@example.com)"
}


# ------------- Helpers -------------

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
    tokens = re.findall(r"\b\w+\b", text.lower())
    return tokens


def load_seen_ids(out_path: Path) -> set:
    """If output file exists, collect all unique_entity_identifier already written."""
    seen = set()
    if not out_path.exists():
        return seen

    with out_path.open("r", encoding="utf-8") as f:
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

    # get JSON with metadata
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
                meta["http_status"] = 200  # trafilatura hides raw status
                meta["final_url"] = url
                meta["article_title"] = title
                return text, meta
        except Exception as e:
            meta["fetch_status"] = "error"
            meta["error_reason"] = f"trafilatura_error: {e}"

    # Fallback: raw requests + naive extraction (all visible text)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        meta["http_status"] = resp.status_code
        meta["final_url"] = str(resp.url)

        if resp.status_code != 200:
            meta["fetch_status"] = "http_error"
            meta["error_reason"] = f"HTTP {resp.status_code}"
            return "", meta

        html = resp.text

        # Very naive text extraction: strip tags manually
        # (You can upgrade to BeautifulSoup/newspaper later.)
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


# ------------- Main crawl logic -------------

def crawl_articles(input_path: Path, output_path: Path):
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    # Count total lines for progress
    total = 0
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                total += 1

    print(f"[info] Input: {input_path} (~{total} records)")
    print(f"[info] Output: {output_path}")

    seen_ids = load_seen_ids(output_path)
    if seen_ids:
        print(f"[info] Resuming: {len(seen_ids)} IDs already in {output_path}")

    processed = 0
    written = 0

    with input_path.open("r", encoding="utf-8") as fin, \
         output_path.open("a", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue

            uid = row.get("unique_entity_identifier")
            url = row.get("url")

            if not uid or not url:
                # skip rows without necessary info
                continue

            processed += 1

            if uid in seen_ids:
                print_progress("[crawl] Progress", processed, total)
                continue

            # Fetch + extract
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

            fout.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
            written += 1
            seen_ids.add(uid)

            print_progress("[crawl] Progress", processed, total)
            time.sleep(REQUEST_SLEEP)

    print(f"\n[done] Processed {processed} records, wrote {written} new crawls to {output_path}")


def main():
    ap = argparse.ArgumentParser(description="Phase 4: Crawl article pages into JSON keyed by unique_entity_identifier.")
    ap.add_argument("--input", type=str, default=DEFAULT_INPUT,
                    help="Input NDJSON file with unique_entity_identifier + url (default: articles_with_id.ndjson)")
    ap.add_argument("--output", type=str, default=DEFAULT_OUTPUT,
                    help="Output NDJSON file for crawl results (default: crawl_raw.ndjson)")
    args = ap.parse_args()

    crawl_articles(Path(args.input), Path(args.output))


if __name__ == "__main__":
    main()
