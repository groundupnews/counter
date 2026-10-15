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
import re
from datetime import date, datetime
from analyze_pixel import DB_PATH, open_db

from flask import Flask, abort, make_response, render_template, request

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

# Pixel labels for republished articles are prefixed with the article's pk,
# e.g. "17291_e_tired-waiting-for-government-pe".
LABEL_PK = re.compile(r"^(\d+)_")

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

    conn = open_db(db_path)

    EXCLUDE_PIXELS = ("robots.txt", ".env")

    placeholders = ",".join("?" * len(exclude_sites))
    exclude_clause = f"AND domain NOT IN ({placeholders})" if exclude_sites else ""

    rows = conn.execute(
        f"""
        SELECT h.pixel, h.domain, SUM(h.count) AS total,
               (
                   SELECT ru.url FROM referrer_urls ru
                   WHERE ru.pixel = h.pixel AND ru.domain = h.domain
                   ORDER BY ru.count DESC
                   LIMIT 1
               ) AS url
        FROM hits h
        WHERE h.date >= ? AND h.date <= ?
        AND h.pixel NOT IN ({",".join("?" * len(EXCLUDE_PIXELS))})
        {exclude_clause}
        AND h.domain != '(no referrer)'
        GROUP BY h.pixel, h.domain
        ORDER BY total DESC
        LIMIT {TOP_N}
    """,
        (date_from, date_to, *EXCLUDE_PIXELS, *exclude_sites),
    ).fetchall()

    conn.close()

    results = []
    for pixel, domain, total, url in rows:
        label = os.path.splitext(pixel)[0]
        match = LABEL_PK.match(label)
        results.append(
            {
                "label": label,
                "pk": match.group(1) if match else None,
                "referrer": domain,
                "hits": total,
                "url": url,
                "search_term": f"{label} site:{domain}",
            }
        )
    return results


# ── Stats routes ──────────────────────────────────────────────────────────────


@app.route("/stats/")
@app.route("/stats")
def stats_form():
    return render_template(
        "form.html",
        today=date.today().strftime("%Y-%m-%d"),
        default_excludes=EXCLUDE_SITES,
    )


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

    fmt = lambda d: datetime.strptime(d, "%Y-%m-%d").strftime("%-d %B %Y")
    return render_template(
        "report.html",
        rows=rows,
        top_n=TOP_N,
        date_from_label=fmt(date_from),
        date_to_label=fmt(date_to),
        total_hits=sum(r["hits"] for r in rows),
        exclude_sites=exclude_sites,
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
    )


@app.route("/")
def index():
    abort(404)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Pixel counter — serves tracking pixels and stats reports."
    )
    parser.add_argument("--db", default=DB_PATH, help="Path to the SQLite database.")
    parser.add_argument("--port", type=int, default=5000, help="Port to listen on.")
    args = parser.parse_args()

    app.config["DB_PATH"] = args.db
    app.run(host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
