"""
Pixel tracking log analyser with SQLite state tracking.

Counts hits per republished article per referring site, storing results in
SQLite so each log line is counted exactly once across multiple runs and
across log rotation.

Usage
-----
Normal run (process new lines since last run):
    python analyse_pixel_log.py

After logrotate has rotated the file (drain the rotated file then reset):
    python analyse_pixel_log.py --rotated /var/log/nginx/pixel_access.log.1

Print the current report without processing any log:
    python analyse_pixel_log.py --report

Options
-------
--log PATH       Path to the live log file (default: LOG_PATH constant below)
--rotated PATH   Path to the just-rotated log file; process it to EOF then
                 clear state so the new live log starts fresh
--report         Print the accumulated report and exit
--db PATH        Path to the SQLite database (default: DB_PATH constant below)
--reset          Wipe all data and state (use with care)

Logrotate integration
---------------------
Add a postrotate block to your logrotate config:

    /var/log/nginx/pixel_access.log {
        daily
        missingok
        notifempty
        postrotate
            python /opt/scripts/analyse_pixel_log.py \\
                --rotated /var/log/nginx/pixel_access.log.1
        endscript
    }
"""

import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from urllib.parse import urlparse

# ── Configuration ─────────────────────────────────────────────────────────────

LOG_PATH = "/var/log/nginx/counter_access.log"
DB_PATH = "/home/gu/counter/counter.db"

# ── Log parsing ───────────────────────────────────────────────────────────────

LOG_PATTERN = re.compile(r'(\S+)\s+"GET (\S+) HTTP/[\d.]+"\s+"([^"]*)"')


def extract_domain(url: str) -> str:
    if not url or url == "-":
        return "(no referrer)"
    try:
        domain = urlparse(url).netloc
        return domain.lower() if domain else url.lower()
    except Exception:
        return "(unknown)"


def parse_pixel_name(path_part: str) -> str:
    """Return just the filename portion of the request path."""
    return path_part.rstrip("/").split("/")[-1]


# ── Database ──────────────────────────────────────────────────────────────────


def open_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hits (
            pixel   TEXT    NOT NULL,
            domain  TEXT    NOT NULL,
            date    TEXT    NOT NULL,
            count   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (pixel, domain, date)
        );

        CREATE TABLE IF NOT EXISTS log_state (
            log_path TEXT PRIMARY KEY,
            inode    INTEGER NOT NULL,
            offset   INTEGER NOT NULL
        );
    """)
    conn.commit()
    return conn


def load_state(conn: sqlite3.Connection, log_path: str):
    """Return (inode, offset) for log_path, or (None, 0) if unseen."""
    row = conn.execute(
        "SELECT inode, offset FROM log_state WHERE log_path = ?", (log_path,)
    ).fetchone()
    return (row[0], row[1]) if row else (None, 0)


def save_state(
    conn: sqlite3.Connection, log_path: str, inode: int, offset: int
) -> None:
    conn.execute(
        """
        INSERT INTO log_state (log_path, inode, offset)
        VALUES (?, ?, ?)
        ON CONFLICT(log_path) DO UPDATE SET inode=excluded.inode,
                                            offset=excluded.offset
    """,
        (log_path, inode, offset),
    )
    conn.commit()


def clear_state(conn: sqlite3.Connection, log_path: str) -> None:
    conn.execute("DELETE FROM log_state WHERE log_path = ?", (log_path,))
    conn.commit()


def upsert_hits(conn: sqlite3.Connection, batch: dict) -> None:
    """batch: { (pixel, domain, date): count }"""
    conn.executemany(
        """
        INSERT INTO hits (pixel, domain, date, count) VALUES (?, ?, ?, ?)
        ON CONFLICT(pixel, domain, date) DO UPDATE SET count = count + excluded.count
    """,
        [
            (pixel, domain, date, count)
            for (pixel, domain, date), count in batch.items()
        ],
    )
    conn.commit()


# ── Core processing ───────────────────────────────────────────────────────────


def process_file(conn: sqlite3.Connection, file_path: str, start_offset: int) -> int:
    """
    Read file_path from start_offset to EOF, accumulate hit counts into the
    database, and return the new EOF offset.
    """
    batch = defaultdict(int)
    skipped = 0

    with open(file_path, encoding="utf-8", errors="replace") as fh:
        fh.seek(start_offset)
        for line in fh:
            line = line.strip()
            if not line:
                continue
            match = LOG_PATTERN.search(line)
            if not match:
                skipped += 1
                continue
            date, path_part, referrer = match.groups()
            # Normalise to YYYY-MM-DD (log emits full ISO timestamp)
            date = date[:10]
            pixel = parse_pixel_name(path_part)
            domain = extract_domain(referrer)
            batch[(pixel, domain, date)] += 1

        new_offset = fh.tell()

    if batch:
        upsert_hits(conn, batch)

    lines_processed = sum(batch.values())
    print(
        f"  Processed {lines_processed} hit(s) from '{file_path}'"
        f"  (offset {start_offset} -> {new_offset})"
    )
    if skipped:
        print(f"  Skipped {skipped} unrecognised line(s).")

    return new_offset


def run_normal(conn: sqlite3.Connection, log_path: str) -> None:
    """Process new lines in the live log file since the last run."""
    if not os.path.exists(log_path):
        print(f"Log file not found: {log_path}")
        return

    current_inode = os.stat(log_path).st_ino
    stored_inode, stored_offset = load_state(conn, log_path)

    if stored_inode is not None and stored_inode != current_inode:
        # The inode changed but --rotated was never called (e.g. postrotate
        # is not yet configured). Start the new file from the top.
        print(
            "  Warning: inode changed since last run. "
            "Was --rotated called at rotation time? Starting from offset 0."
        )
        stored_offset = 0

    new_offset = process_file(conn, log_path, stored_offset)
    save_state(conn, log_path, current_inode, new_offset)


def run_rotated(
    conn: sqlite3.Connection, rotated_path: str, live_log_path: str
) -> None:
    """
    Drain the rotated file from its last saved offset to EOF, then clear
    the state entry for the live log so the next normal run starts at 0.
    """
    if not os.path.exists(rotated_path):
        print(f"Rotated log file not found: {rotated_path}")
        return

    # The rotated file has the same inode as the live log had before rotation,
    # so we look up the state under the live log path.
    _, stored_offset = load_state(conn, live_log_path)

    process_file(conn, rotated_path, stored_offset)

    # Clear state so the now-empty live log is treated as fresh on next run.
    clear_state(conn, live_log_path)
    print(f"  State cleared for '{live_log_path}'. Next run starts at offset 0.")


# ── Report ────────────────────────────────────────────────────────────────────


def print_report(conn: sqlite3.Connection) -> None:
    rows = conn.execute("""
        SELECT pixel, domain, SUM(count) as count
        FROM hits
        GROUP BY pixel, domain
        ORDER BY pixel, SUM(count) DESC
    """).fetchall()

    if not rows:
        print("No hits recorded yet.")
        return

    articles = defaultdict(list)
    for pixel, domain, count in rows:
        articles[pixel].append((domain, count))

    article_totals = {
        pixel: sum(c for _, c in domains) for pixel, domains in articles.items()
    }

    print("=" * 62)
    print("REPUBLICATION HIT REPORT")
    print("=" * 62)

    for pixel, total in sorted(
        article_totals.items(), key=lambda x: x[1], reverse=True
    ):
        print(f"\nArticle : {pixel}")
        print(f"Total   : {total} hit(s)")
        print(f"  {'Referring site':<44} {'Hits':>6}")
        print(f"  {'-' * 44} {'-' * 6}")
        for domain, count in articles[pixel]:
            print(f"  {domain:<44} {count:>6}")

    print("\n" + "=" * 62)
    print(f"Articles tracked : {len(articles)}")
    print(f"Total hits       : {sum(article_totals.values())}")
    print("=" * 62)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Pixel tracking log analyser with SQLite state tracking."
    )
    parser.add_argument("--log", default=LOG_PATH, help="Path to the live log file.")
    parser.add_argument(
        "--rotated", metavar="PATH", help="Path to the just-rotated log file."
    )
    parser.add_argument("--db", default=DB_PATH, help="Path to the SQLite database.")
    parser.add_argument(
        "--report", action="store_true", help="Print the accumulated report and exit."
    )
    parser.add_argument(
        "--reset", action="store_true", help="Wipe all data and state (use with care)."
    )
    args = parser.parse_args()

    conn = open_db(args.db)

    if args.reset:
        conn.executescript("DELETE FROM hits; DELETE FROM log_state;")
        conn.commit()
        print("Database reset.")
        return

    if args.report:
        print_report(conn)
        return

    if args.rotated:
        print(f"-- Draining rotated log: {args.rotated}")
        run_rotated(conn, args.rotated, args.log)
    else:
        print(f"-- Processing live log: {args.log}")
        run_normal(conn, args.log)

    print()
    print_report(conn)


if __name__ == "__main__":
    main()
