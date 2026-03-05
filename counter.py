"""
Pixel counter — combined pixel serving and republication stats.

Routes
------
GET /pixels/<name>          Serve a 1x1 transparent GIF (the tracking pixel)
GET /stats/                 Date range + exclusion form
GET /stats/YYYYMMDD/YYYYMMDD[?exclude=domain&exclude=domain]
                            HTML report of top 50 republished articles

Usage
-----
    python counter.py
    python counter.py --db /var/lib/pixel-tracker/pixel_hits.db
    python counter.py --port 5000

Via Gunicorn:
    gunicorn --workers 2 --bind 127.0.0.1:5000 --chdir /path/to/folder counter:app
"""

import argparse
import os
import sqlite3
from datetime import date, datetime
from analyze_pixel import DB_PATH

from flask import Flask, abort, make_response, request

# ── Default sites to exclude from stats reports ───────────────────────────────
# These are pre-filled in the form but can be overridden per-request.

EXCLUDE_SITES = [
    "groundup.org.za",
    "www.groundup.org.za",
    "groundup.news",
    "www.groundup.news",
]

# ── Configuration ─────────────────────────────────────────────────────────────

TOP_N = 200

# ── Tracking pixel ────────────────────────────────────────────────────────────
# Complete binary content of a 1x1 transparent GIF — fixed at startup,
# never regenerated per request.

TRANSPARENT_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00"
    b"\xff\xff\xff\x00\x00\x00!\xf9\x04"
    b"\x00\x00\x00\x00\x00,\x00\x00\x00"
    b"\x00\x01\x00\x01\x00\x00\x02\x02"
    b"D\x01\x00;"
)

# ── App ───────────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.config["DB_PATH"] = DB_PATH


# ── Pixel route ───────────────────────────────────────────────────────────────


@app.route("/pixels/<path:pixel_name>")
def serve_pixel(pixel_name):
    response = make_response(TRANSPARENT_GIF)
    response.headers["Content-Type"] = "image/gif"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ── Stats data ────────────────────────────────────────────────────────────────


def query_hits(db_path: str, date_from: str, date_to: str, exclude_sites: list) -> list:
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)

    placeholders = ",".join("?" * len(exclude_sites))
    exclude_clause = f"AND domain NOT IN ({placeholders})" if exclude_sites else ""

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
        (date_from, date_to, *exclude_sites),
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


# ── Stats HTML ────────────────────────────────────────────────────────────────

SHARED_STYLES = """
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
        --ink:    #0f0f0f;
        --paper:  #f5f0e8;
        --rule:   #c8bfaa;
        --accent: #c1370a;
        --muted:  #7a7060;
        --col-w:  860px;
    }

    body {
        background: var(--paper);
        color: var(--ink);
        font-family: 'IBM Plex Sans', sans-serif;
        min-height: 100vh;
    }

    .kicker {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 0.6rem;
    }

    h1 {
        font-family: 'Playfair Display', serif;
        font-weight: 900;
        letter-spacing: -0.02em;
        line-height: 1.05;
    }
"""

FONT_LINK = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link href="https://fonts.googleapis.com/css2?family=Playfair+Display'
    ":wght@700;900&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans"
    ':wght@400;500&display=swap" rel="stylesheet">'
)


def render_form() -> str:
    today = date.today().strftime("%Y-%m-%d")
    default_excludes = "\n".join(EXCLUDE_SITES)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Republication Stats</title>
    {FONT_LINK}
    <style>
        {SHARED_STYLES}

        body {{
            display: flex;
            align-items: flex-start;
            justify-content: center;
            padding: 4rem 1.5rem;
        }}

        .card {{
            width: 100%;
            max-width: 480px;
        }}

        h1 {{
            font-size: 2.2rem;
            margin-bottom: 2rem;
            padding-bottom: 1.2rem;
            border-bottom: 2px solid var(--ink);
        }}

        .field {{ margin-bottom: 1.5rem; }}

        label {{
            display: block;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.68rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: var(--muted);
            margin-bottom: 0.4rem;
        }}

        label .required {{ color: var(--accent); margin-left: 0.2rem; }}

        input[type="date"], textarea {{
            width: 100%;
            background: white;
            border: 1px solid var(--rule);
            border-radius: 2px;
            padding: 0.6rem 0.75rem;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.85rem;
            color: var(--ink);
            transition: border-color 0.15s;
            appearance: none;
        }}

        input[type="date"]:focus, textarea:focus {{
            outline: none;
            border-color: var(--accent);
        }}

        textarea {{ resize: vertical; min-height: 90px; line-height: 1.6; }}

        .hint {{
            margin-top: 0.35rem;
            font-size: 0.72rem;
            color: var(--muted);
            line-height: 1.5;
        }}

        button {{
            margin-top: 0.5rem;
            width: 100%;
            background: var(--ink);
            color: var(--paper);
            border: none;
            border-radius: 2px;
            padding: 0.8rem 1.5rem;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.8rem;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            cursor: pointer;
            transition: background 0.15s;
        }}

        button:hover {{ background: var(--accent); }}
    </style>
</head>
<body>
    <div class="card">
        <p class="kicker">Republication Analytics</p>
        <h1>Run a Stats Report</h1>
        <form id="statsForm">
            <div class="field">
                <label>Start date <span class="required">*</span></label>
                <input type="date" id="date_from" value="{today}" required>
            </div>
            <div class="field">
                <label>End date <span class="required">*</span></label>
                <input type="date" id="date_to" value="{today}" required>
            </div>
            <div class="field">
                <label>Exclude referrers</label>
                <textarea id="excludes" placeholder="one domain per line">{default_excludes}</textarea>
                <p class="hint">One domain per line. Leave blank to include all referrers.</p>
            </div>
            <button type="submit">View Report &rarr;</button>
        </form>
    </div>
    <script>
        document.getElementById('statsForm').addEventListener('submit', function(e) {{
            e.preventDefault();
            const from = document.getElementById('date_from').value.replace(/-/g, '');
            const to   = document.getElementById('date_to').value.replace(/-/g, '');
            const excludes = document.getElementById('excludes').value
                .split('\\n')
                .map(s => s.trim())
                .filter(s => s.length > 0);
            const params = new URLSearchParams();
            excludes.forEach(site => params.append('exclude', site));
            const query = params.toString() ? '?' + params.toString() : '';
            window.location.href = `/stats/${{from}}/${{to}}${{query}}`;
        }});
    </script>
</body>
</html>"""


def render_report(rows: list, date_from: str, date_to: str, exclude_sites: list) -> str:
    fmt = lambda d: datetime.strptime(d, "%Y-%m-%d").strftime("%-d %B %Y")
    total_hits = sum(r["hits"] for r in rows)

    rows_html = "".join(
        f"""
        <tr>
            <td class="rank">{i}</td>
            <td class="label">{row["label"]}</td>
            <td class="referrer">{row["referrer"]}</td>
            <td class="hits">{row["hits"]:,}</td>
        </tr>"""
        for i, row in enumerate(rows, 1)
    )

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
    {FONT_LINK}
    <style>
        {SHARED_STYLES}

        body {{ padding: 3rem 1.5rem 6rem; }}

        header {{
            max-width: var(--col-w);
            margin: 0 auto 3rem;
            border-top: 3px solid var(--ink);
            padding-top: 1.5rem;
        }}

        h1 {{
            font-size: clamp(2rem, 5vw, 3.2rem);
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

        .meta-item strong {{ color: var(--ink); font-weight: 500; }}

        .back-link {{
            display: inline-block;
            margin-bottom: 1.5rem;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.72rem;
            letter-spacing: 0.06em;
            color: var(--accent);
            text-decoration: none;
            text-transform: uppercase;
        }}

        .back-link:hover {{ text-decoration: underline; }}

        main {{ max-width: var(--col-w); margin: 0 auto; }}

        table {{ width: 100%; border-collapse: collapse; }}

        thead tr {{ border-bottom: 2px solid var(--ink); }}

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

        th.hits, td.hits {{ text-align: right; }}
        th.rank, td.rank {{ text-align: right; width: 2.5rem; }}

        tbody tr {{
            border-bottom: 1px solid var(--rule);
            transition: background 0.12s;
        }}

        tbody tr:hover {{ background: rgba(193, 55, 10, 0.04); }}

        td {{ padding: 0.75rem; font-size: 0.875rem; vertical-align: middle; }}

        td.rank {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.7rem;
            color: var(--rule);
        }}

        td.label {{
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.78rem;
            max-width: 340px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}

        td.referrer {{ color: var(--accent); font-size: 0.85rem; }}

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
        <a class="back-link" href="/stats/">&larr; New report</a>
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
        &nbsp;·&nbsp; Excluding: {", ".join(exclude_sites) if exclude_sites else "nothing"}
    </footer>
</body>
</html>"""


# ── Stats routes ──────────────────────────────────────────────────────────────


@app.route("/stats/")
@app.route("/stats")
def stats_form():
    return render_form()


@app.route("/stats/<string:from_str>/<string:to_str>")
def stats_report(from_str: str, to_str: str):
    try:
        date_from = datetime.strptime(from_str, "%Y%m%d").strftime("%Y-%m-%d")
        date_to = datetime.strptime(to_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        abort(400, "Dates must be in YYYYMMDD format, e.g. /stats/20260101/20260131")

    if date_from > date_to:
        abort(400, "date_from must be on or before date_to.")

    exclude_sites = request.args.getlist("exclude") or ""
    rows = query_hits(app.config["DB_PATH"], date_from, date_to, exclude_sites)
    return render_report(rows, date_from, date_to, exclude_sites)


@app.route("/")
def index():
    abort(404)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Pixel counter — serves tracking pixels and stats reports."
    )
    parser.add_argument("--db", default=DB_PATH, help="Path to the SQLite database.")
    args = parser.parse_args()

    app.config["DB_PATH"] = args.db
    app.run(host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
