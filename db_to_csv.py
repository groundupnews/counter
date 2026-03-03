"""
Query top 50 (referrer, article) pairs from the pixel hits database,
filtered by the date the hits were recorded.

Usage:
    python db_to_csv.py 20260101 20260131
    python db_to_csv.py 20260101 20260131 --db /var/lib/pixel-tracker/pixel_hits.db
    python db_to_csv.py 20260101 20260131 --out results.csv

Output columns:
    referrer  - referring domain (e.g. example.com)
    label     - article slug without extension (e.g. a-cool-slug-2026-01-15)
    hits      - total hit count over the date range
"""

import argparse
import csv
import os
import sqlite3
import sys
from datetime import datetime

DB_PATH = "/home/gu/counter/counter.db"


def parse_arg_date(s: str) -> str:
    """Validate YYYYMMDD and return as YYYY-MM-DD string for SQLite."""
    try:
        return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{s}' — expected YYYYMMDD, e.g. 20260101"
        )


def strip_extension(pixel: str) -> str:
    return os.path.splitext(pixel)[0]


def query_hits(db_path: str, date_from: str, date_to: str) -> list:
    if not os.path.exists(db_path):
        print(f"Error: database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)

    rows = conn.execute("""
        SELECT pixel, domain, SUM(count) as total
        FROM hits
        WHERE date >= ? AND date <= ?
        GROUP BY pixel, domain
        ORDER BY total DESC
        LIMIT 50
    """, (date_from, date_to)).fetchall()

    conn.close()

    return [
        {
            "referrer": domain,
            "label":    strip_extension(pixel),
            "hits":     total,
        }
        for pixel, domain, total in rows
    ]


def write_csv(results: list, out_path: str | None) -> None:
    if out_path:
        fh = open(out_path, "w", newline="", encoding="utf-8")
        destination = out_path
    else:
        fh = sys.stdout
        destination = "stdout"

    writer = csv.DictWriter(fh, fieldnames=["referrer", "label", "hits"])
    writer.writeheader()
    writer.writerows(results)

    if out_path:
        fh.close()
        print(f"Wrote {len(results)} row(s) to {destination}")


def main():
    parser = argparse.ArgumentParser(
        description="Export top 50 pixel hits for a date range to CSV."
    )
    parser.add_argument("date_from", type=parse_arg_date,
                        help="Start date, inclusive (YYYYMMDD)")
    parser.add_argument("date_to",   type=parse_arg_date,
                        help="End date, inclusive (YYYYMMDD)")
    parser.add_argument("--db",  default=DB_PATH,
                        help="Path to the SQLite database.")
    parser.add_argument("--out", metavar="PATH",
                        help="Output CSV file (default: print to stdout).")
    args = parser.parse_args()

    if args.date_from > args.date_to:
        print("Error: date_from must be on or before date_to.")
        sys.exit(1)

    results = query_hits(args.db, args.date_from, args.date_to)

    if not results:
        print("No matching hits found for that date range.")
        sys.exit(0)

    write_csv(results, args.out)


if __name__ == "__main__":
    main()
