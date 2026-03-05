"""
Pixel hit stats server.

Serves an HTML report at /stats/YYYYMMDD/YYYYMMDD showing the top 50
(article, referrer) pairs for the given date range, excluding configured sites.

Usage:
    python stats_server.py
    python stats_server.py --db /var/lib/pixel-tracker/pixel_hits.db
    python stats_server.py --port 5001

Then visit e.g.:
    http://localhost:5001/stats/20260101/20260131
"""

import argparse
import os
import sqlite3
from datetime import datetime

from flask import Flask, abort

# ── Sites to exclude from the report ─────────────────────────────────────────
# Add any referring domains here that you want to hide from the stats view.

EXCLUDE_SITES = [
    "groundup.org.za",
    "www.groundup.org.za",
]

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = "/var/lib/pixel-tracker/pixel_hits.db"
DEFAULT_PORT = 5001
TOP_N = 50

# ── App ───────────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.config["DB_PATH"] = DEFAULT_DB_PATH


# ── Data ──────────────────────────────────────────────────────────────────────


def query_hits(db_path: str, date_from: str, date_to: str) -> list:
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)

    placeholders = ",".join("?" * len(EXCLUDE_SITES))
    exclude_clause = f"AND domain NOT IN ({placeholders})" if EXCLUDE_SITES else ""

    rows = conn.execute(
        f"""
        SELECT pixel, domain, SUM(count) AS total
        FROM hits
        WHERE date >= ? AND date <= ?
        {exclude_clause}
        GROUP BY pixel, domain
        ORDER BY total DESC
        LIMIT {TOP_N}
    """,
        (date_from, date_to, *EXCLUDE_SITES),
    ).fetchall()

    conn.close()

    return [
        {
            "label": os.path.splitext(pixel)[0],
            "referrer": domain,
            "hits": total,
        }
        for pixel, domain, total in rows
    ]


# ── HTML ──────────────────────────────────────────────────────────────────────


def render_page(rows: list, date_from: str, date_to: str) -> str:
    fmt = lambda d: datetime.strptime(d, "%Y-%m-%d").strftime("%-d %B %Y")
    total_hits = sum(r["hits"] for r in rows)

    rows_html = ""
    for i, row in enumerate(rows, 1):
        rows_html += f"""
        <tr>
            <td class="rank">{i}</td>
            <td class="label">{row["label"]}</td>
            <td class="referrer">{row["referrer"]}</td>
            <td class="hits">{row["hits"]:,}</td>
        </tr>"""

    empty_html = (
        """
        <tr class="empty-row">
            <td colspan="4">No hits found for this date range.</td>
        </tr>"""
        if not rows
        else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Republication Stats</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500&display=swap" rel="stylesheet">
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

        :root {{
            --ink:      #0f0f0f;
            --paper:    #f5f0e8;
            --rule:     #c8bfaa;
            --accent:   #c1370a;
            --muted:    #7a7060;
            --col-w:    860px;
        }}

        body {{
            background: var(--paper);
            color: var(--ink);
            font-family: 'IBM Plex Sans', sans-serif;
            min-height: 100vh;
            padding: 3rem 1.5rem 6rem;
        }}

        header {{
            max-width: var(--col-w);
            margin: 0 auto 3rem;
            border-top: 3px solid var(--ink);
            padding-top: 1.5rem;
        }}

        .kicker {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.7rem;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: var(--accent);
            margin-bottom: 0.6rem;
        }}

        h1 {{
            font-family: 'Playfair Display', serif;
            font-size: clamp(2rem, 5vw, 3.2rem);
            font-weight: 900;
            line-height: 1.05;
            letter-spacing: -0.02em;
            margin-bottom: 1rem;
        }}

        .meta {{
            display: flex;
            gap: 2rem;
            flex-wrap: wrap;
            border-top: 1px solid var(--rule);
            padding-top: 0.9rem;
            margin-top: 0.9rem;
        }}

        .meta-item {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.72rem;
            color: var(--muted);
            letter-spacing: 0.04em;
        }}

        .meta-item strong {{
            color: var(--ink);
            font-weight: 500;
        }}

        main {{
            max-width: var(--col-w);
            margin: 0 auto;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        thead tr {{
            border-bottom: 2px solid var(--ink);
        }}

        th {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.65rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: var(--muted);
            padding: 0 0.75rem 0.6rem;
            text-align: left;
            font-weight: 500;
        }}

        th.hits, td.hits {{
            text-align: right;
        }}

        th.rank, td.rank {{
            text-align: right;
            width: 2.5rem;
        }}

        tbody tr {{
            border-bottom: 1px solid var(--rule);
            transition: background 0.12s;
        }}

        tbody tr:hover {{
            background: rgba(193, 55, 10, 0.04);
        }}

        td {{
            padding: 0.75rem 0.75rem;
            font-size: 0.875rem;
            vertical-align: middle;
        }}

        td.rank {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.7rem;
            color: var(--rule);
        }}

        td.label {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.78rem;
            color: var(--ink);
            max-width: 340px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}

        td.referrer {{
            color: var(--accent);
            font-size: 0.85rem;
        }}

        td.hits {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.82rem;
            font-weight: 500;
            letter-spacing: 0.02em;
            white-space: nowrap;
        }}

        tr.empty-row td {{
            text-align: center;
            padding: 3rem;
            color: var(--muted);
            font-style: italic;
        }}

        footer {{
            max-width: var(--col-w);
            margin: 2.5rem auto 0;
            padding-top: 1rem;
            border-top: 1px solid var(--rule);
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.65rem;
            color: var(--muted);
            letter-spacing: 0.04em;
        }}
    </style>
</head>
<body>
    <header>
        <p class="kicker">Republication Analytics</p>
        <h1>Top {TOP_N} Republished Articles</h1>
        <div class="meta">
            <span class="meta-item">Period &nbsp;<strong>{fmt(date_from)} – {fmt(date_to)}</strong></span>
            <span class="meta-item">Articles &nbsp;<strong>{len(rows)}</strong></span>
            <span class="meta-item">Total hits &nbsp;<strong>{total_hits:,}</strong></span>
        </div>
    </header>
    <main>
        <table>
            <thead>
                <tr>
                    <th class="rank">#</th>
                    <th>Article</th>
                    <th>Referring site</th>
                    <th class="hits">Hits</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
                {empty_html}
            </tbody>
        </table>
    </main>
    <footer>
        Generated {datetime.utcnow().strftime("%Y-%m-%d %H:%M")} UTC
        &nbsp;·&nbsp; Excluding: {", ".join(EXCLUDE_SITES) if EXCLUDE_SITES else "nothing"}
    </footer>
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────


@app.route("/stats/<string:from_str>/<string:to_str>")
def stats(from_str: str, to_str: str):
    try:
        date_from = datetime.strptime(from_str, "%Y%m%d").strftime("%Y-%m-%d")
        date_to = datetime.strptime(to_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        abort(400, "Dates must be in YYYYMMDD format, e.g. /stats/20260101/20260131")

    if date_from > date_to:
        abort(400, "date_from must be on or before date_to.")

    rows = query_hits(app.config["DB_PATH"], date_from, date_to)
    return render_page(rows, date_from, date_to)


@app.route("/")
def index():
    abort(404)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Pixel hit stats server.")
    parser.add_argument(
        "--db", default=DEFAULT_DB_PATH, help="Path to the SQLite database."
    )
    parser.add_argument(
        "--port", default=DEFAULT_PORT, type=int, help="Port to listen on."
    )
    args = parser.parse_args()

    app.config["DB_PATH"] = args.db
    app.run(host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
