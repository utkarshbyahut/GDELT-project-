#!/usr/bin/env python3
"""
Add a stable primary key ("unique_entity_identifier") to both articles.csv
and articles.ndjson, based on (disasterNumber, county_fips, url).

PK format:
    "<disasterNumber>_<county_fips>_<sha1(url)[:10]>"

Outputs:
    - articles_with_id.csv
    - articles_with_id.ndjson
    - pk_generation.log   (batch progress log)

Run:
    python add_unique_ids.py
"""

import argparse
import csv
import hashlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


BATCH_SIZE = 1000
LOG_FILE = "pk_generation.log"


def make_row_id(disaster_number: Optional[str],
                county_fips: Optional[str],
                url: Optional[str],
                fallback_suffix: str) -> Tuple[str, Optional[str]]:
    """
    Build the unique_entity_identifier.

    Returns (row_id, warning_msg_or_None)
    """
    dn = (str(disaster_number).strip() if disaster_number is not None else "")
    cf = (str(county_fips).strip() if county_fips is not None else "")
    u = (url or "").strip()

    if not dn or not cf or not u:
        # Missing some key piece; still make a row_id but log a warning
        row_id = f"missingpk_{fallback_suffix}"
        warn = f"Missing key fields for row {fallback_suffix}: disasterNumber={dn}, county_fips={cf}, url={u!r}"
        return row_id, warn

    h = hashlib.sha1(u.encode("utf-8")).hexdigest()[:10]
    row_id = f"{dn}_{cf}_{h}"
    return row_id, None


def count_lines(path: Path) -> int:
    """Count non-empty lines in a text file."""
    total = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                total += 1
    return total


def log_batch(message: str):
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


def print_progress(prefix: str, done: int, total: int):
    """ASCII progress bar."""
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


def process_csv(csv_path: Path, out_path: Path):
    if not csv_path.exists():
        print(f"[csv] input not found: {csv_path}")
        return

    print(f"[csv] Counting rows in {csv_path} ...")
    total_rows = count_lines(csv_path) - 1  # minus header
    if total_rows < 0:
        total_rows = 0

    print(f"[csv] Found ~{total_rows} data rows.")
    log_batch(f"CSV start: {csv_path} with ~{total_rows} rows")

    with csv_path.open("r", newline="", encoding="utf-8") as fin, \
         out_path.open("w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        fieldnames = ["unique_entity_identifier"] + (reader.fieldnames or [])
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        processed = 0
        batch_index = 0

        for idx, row in enumerate(reader):
            dn = row.get("disasterNumber")
            cf = row.get("county_fips")
            url = row.get("url")

            row_id, warn = make_row_id(dn, cf, url, f"csv_{idx}")
            if warn:
                log_batch("[csv warning] " + warn)

            row_out = dict(row)
            row_out["unique_entity_identifier"] = row_id
            writer.writerow(row_out)

            processed += 1
            if processed % BATCH_SIZE == 0:
                batch_index += 1
                log_batch(f"[csv] batch {batch_index} processed {processed} rows")
            print_progress("[csv] Adding primary key", processed, total_rows)

    log_batch(f"[csv] done: wrote {processed} rows to {out_path}")
    print(f"[csv] Done. Wrote {processed} rows to {out_path}")


def process_ndjson(ndjson_path: Path, out_path: Path):
    if not ndjson_path.exists():
        print(f"[ndjson] input not found: {ndjson_path}")
        return

    print(f"[ndjson] Counting rows in {ndjson_path} ...")
    total_rows = count_lines(ndjson_path)
    print(f"[ndjson] Found ~{total_rows} records.")
    log_batch(f"NDJSON start: {ndjson_path} with ~{total_rows} rows")

    with ndjson_path.open("r", encoding="utf-8") as fin, \
         out_path.open("w", encoding="utf-8") as fout:

        processed = 0
        batch_index = 0

        for idx, line in enumerate(fin):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                log_batch(f"[ndjson error] line {idx}: {e}")
                continue

            dn = obj.get("disasterNumber")
            cf = obj.get("county_fips")
            url = obj.get("url")

            row_id, warn = make_row_id(dn, cf, url, f"ndjson_{idx}")
            if warn:
                log_batch("[ndjson warning] " + warn)

            obj["unique_entity_identifier"] = row_id
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

            processed += 1
            if processed % BATCH_SIZE == 0:
                batch_index += 1
                log_batch(f"[ndjson] batch {batch_index} processed {processed} rows")
            print_progress("[ndjson] Adding primary key", processed, total_rows)

    log_batch(f"[ndjson] done: wrote {processed} rows to {out_path}")
    print(f"[ndjson] Done. Wrote {processed} rows to {out_path}")


def main():
    ap = argparse.ArgumentParser(
        description="Add unique_entity_identifier to articles.csv and articles.ndjson."
    )
    ap.add_argument("--csv", type=str, default="articles.csv",
                    help="Input CSV file (default: articles.csv)")
    ap.add_argument("--ndjson", type=str, default="articles.ndjson",
                    help="Input NDJSON file (default: articles.ndjson)")
    ap.add_argument("--csv-out", type=str, default="articles_with_id.csv",
                    help="Output CSV file (default: articles_with_id.csv)")
    ap.add_argument("--ndjson-out", type=str, default="articles_with_id.ndjson",
                    help="Output NDJSON file (default: articles_with_id.ndjson)")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    ndjson_path = Path(args.ndjson)
    csv_out = Path(args.csv_out)
    ndjson_out = Path(args.ndjson_out)

    start_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    log_batch(f"=== PK generation run started at {start_ts} ===")

    process_csv(csv_path, csv_out)
    process_ndjson(ndjson_path, ndjson_out)

    end_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    log_batch(f"=== PK generation run finished at {end_ts} ===")


if __name__ == "__main__":
    main()
