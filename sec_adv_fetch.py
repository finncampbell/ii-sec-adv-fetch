#!/usr/bin/env python3
"""
sec_adv_fetch.py — weekly off-server fetch of the SEC Form ADV compilation feeds.

Runs on a GitHub-hosted runner (public repository, free minutes). One job:

    probe   reports.adviserinfo.sec.gov with HEAD requests for the SEC feed,
            today back through the previous 7 days, until a published date is
            found (the SEC regenerates the files daily but the filename date
            can lag)
    for each adviser type (SEC-registered, State-registered):
        download the .xml.gz for that date INTO MEMORY
        POST it to the private server's authenticated ingest endpoint
            (POST /api/v1/pipeline/ingest/sec_adv/file, X-Pipeline-Token);
            the server stores it under data/sec_adv_incoming/ and its 06:00 UTC
            daily run ingests whatever has landed
    exit non-zero if any feed failed to deliver

Why off-server at all: sec.gov's WAF blocks the private server's datacenter
IP (owner's private repo: DEC-012 / covenant #16). Why this repository and
not the owner's private one: GitHub Actions stopped starting jobs on the
private repository on 2026-10-17 (spending limit); public repositories run on
free minutes — the same route the deed fetcher and the Northdata relay took.

What this script never does: write a file here, upload an artifact, print
anything but filenames / byte counts / HTTP statuses. The feeds are public
regulatory data (firm-level: names, CRD numbers, regulatory AUM) and the
server already de-duplicates: a feed it has seen answers "already_have",
which this script treats as success, so re-runs are safe no-ops.

Environment (set by the workflow; secrets by the repository owner):
    SERVER_URL, PIPELINE_INGEST_TOKEN     required (run mode)
    PROBE_DAYS=8                           dates tried, today backwards
    HTTP_TIMEOUT_S=60  DOWNLOAD_TIMEOUT_S=600  UPLOAD_TIMEOUT_S=600

Modes:
    python sec_adv_fetch.py            # probe → download → upload (needs secrets)
    python sec_adv_fetch.py --probe    # HEAD probe only: feed date, Content-Length
                                       # per feed, statuses; no server, no secrets
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

log = logging.getLogger("sec_adv_fetch")

REPORTS_BASE = "https://reports.adviserinfo.sec.gov/reports/CompilationReports"
INGEST_PATH = "/api/v1/pipeline/ingest/sec_adv/file"
ADVISER_TYPES = ("SEC", "STATE")
# The same honest User-Agent the private workflow used since 2026-07-21: names
# the project and a contact address (the SEC asks automated clients to do so).
USER_AGENT = "investment-intelligence/1.0 (finncampbell@hotmail.co.uk)"
OK_STATUSES = ("received", "already_have")      # the server's two success answers


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


# ── Pure helpers (unit-tested in the private repository) ─────────────────────

def feed_dates(today: date, probe_days: int = 8) -> list[str]:
    """The filename dates to try, newest first: MM_DD_YYYY for today back through probe_days-1 days."""
    return [(today - timedelta(days=i)).strftime("%m_%d_%Y") for i in range(max(1, probe_days))]


def feed_filename(adviser_type: str, date_str: str) -> str:
    """IA_FIRM_<SEC|STATE>_Feed_MM_DD_YYYY.xml.gz — the exact name the server's ingest step parses."""
    return f"IA_FIRM_{adviser_type}_Feed_{date_str}.xml.gz"


def feed_url(adviser_type: str, date_str: str, base: str = REPORTS_BASE) -> str:
    return f"{base}/{feed_filename(adviser_type, date_str)}"


def ingest_url(server_url: str) -> str:
    return server_url.rstrip("/") + INGEST_PATH


def classify_upload(body: object) -> tuple[bool, str]:
    """
    (ok, label) for the server's JSON answer. 'received' = stored,
    'already_have' = duplicate of a file it holds (success — re-runs are no-ops);
    anything else is a failure worth exiting non-zero for.
    """
    if isinstance(body, dict) and body.get("status") in OK_STATUSES:
        return True, str(body.get("status"))
    return False, f"unexpected response: {body!r}"[:200]


# ── Network steps (a session with .head/.get/.post; tests pass a fake) ────────

def probe_feed_date(session, today: date, probe_days: int, timeout: int) -> Optional[str]:
    """First date (newest first) for which the SEC feed answers HEAD 200; None when none does."""
    for date_str in feed_dates(today, probe_days):
        url = feed_url("SEC", date_str)
        r = session.head(url, timeout=timeout, allow_redirects=True)
        log.info("HEAD %s -> %s", feed_filename("SEC", date_str), r.status_code)
        if r.status_code == 200:
            return date_str
    return None


def download_feed(session, adviser_type: str, date_str: str, timeout: int) -> bytes:
    """The whole .xml.gz in memory (feeds are 5–10 MB gz; nothing is written to disk here)."""
    url = feed_url(adviser_type, date_str)
    log.info("GET %s", feed_filename(adviser_type, date_str))
    r = session.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    buf = io.BytesIO()
    for chunk in r.iter_content(chunk_size=1024 * 1024):
        buf.write(chunk)
    data = buf.getvalue()
    log.info("downloaded %s: %.1f MB", feed_filename(adviser_type, date_str), len(data) / 1024 / 1024)
    return data


def upload_feed(session, server_url: str, token: str, filename: str, data: bytes, timeout: int) -> tuple[bool, str]:
    """Multipart POST to the authenticated ingest endpoint; (ok, label) per classify_upload."""
    r = session.post(
        ingest_url(server_url),
        headers={"X-Pipeline-Token": token},
        files={"file": (filename, io.BytesIO(data), "application/gzip")},
        timeout=timeout,
    )
    r.raise_for_status()
    return classify_upload(r.json())


def run(session, server_url: str, token: str, today: date, probe_days: int,
        http_timeout: int, download_timeout: int, upload_timeout: int) -> int:
    """
    One delivery. Returns the number of feeds that failed (0 = all delivered or
    already held by the server); 99 when no feed date was found at all.
    """
    found = probe_feed_date(session, today, probe_days, http_timeout)
    if found is None:
        log.error("no SEC feed found for today or the previous %d days — "
                  "SEC publishing schedule changed or URL pattern broke", probe_days - 1)
        return 99
    log.info("feed date found: %s", found)
    failures = 0
    for adviser_type in ADVISER_TYPES:
        filename = feed_filename(adviser_type, found)
        try:
            data = download_feed(session, adviser_type, found, download_timeout)
        except requests.RequestException as exc:
            # Both feeds are expected for a published date — a missing STATE
            # feed is an upstream anomaly worth failing over.
            log.error("FAIL download %s: %s", filename, str(exc)[:200])
            failures += 1
            continue
        try:
            ok, label = upload_feed(session, server_url, token, filename, data, upload_timeout)
        except (requests.RequestException, ValueError) as exc:
            log.error("FAIL upload %s: %s", filename, str(exc)[:200])
            failures += 1
            continue
        if ok:
            log.info("OK %s -> %s (%d bytes)", filename, label, len(data))
        else:
            log.error("FAIL upload %s: %s", filename, label)
            failures += 1
    return failures


def probe_only(session, today: date, probe_days: int, timeout: int) -> int:
    """--probe: HEAD the SEC feed to find the date, then HEAD both feeds for their sizes. No server."""
    found = probe_feed_date(session, today, probe_days, timeout)
    if found is None:
        log.error("probe: no feed found in %d days", probe_days)
        return 1
    for adviser_type in ADVISER_TYPES:
        r = session.head(feed_url(adviser_type, found), timeout=timeout, allow_redirects=True)
        log.info("probe %s -> %s, Content-Length %s", feed_filename(adviser_type, found),
                 r.status_code, r.headers.get("Content-Length", "?"))
    log.info("probe: feed date %s reachable from this runner", found)
    return 0


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="SEC Form ADV weekly feed fetch (off-server)")
    ap.add_argument("--probe", action="store_true", help="HEAD only; no server, no secrets")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

    today = datetime.now(timezone.utc).date()
    probe_days = env_int("PROBE_DAYS", 8)
    http_timeout = env_int("HTTP_TIMEOUT_S", 60)
    session = make_session()
    if args.probe:
        return probe_only(session, today, probe_days, http_timeout)

    server_url = os.getenv("SERVER_URL", "")
    token = os.getenv("PIPELINE_INGEST_TOKEN", "")
    if not server_url or not token:
        log.error("SERVER_URL and PIPELINE_INGEST_TOKEN are required (repository secrets)")
        return 2
    failures = run(session, server_url, token, today, probe_days, http_timeout,
                   env_int("DOWNLOAD_TIMEOUT_S", 600), env_int("UPLOAD_TIMEOUT_S", 600))
    if failures:
        log.error("FATAL: %d feed(s) failed to deliver", failures)
        return 1
    log.info("all feeds delivered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
