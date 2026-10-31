# ii-sec-adv-fetch

A scheduled GitHub Actions job that fetches the SEC's public Form ADV
compilation feeds (the firm-level IAPD "SEC-registered" and "State-registered"
adviser files that `reports.adviserinfo.sec.gov` publishes daily) once a week
and posts them to a private server, which ingests them. The job runs on
Mondays at 09:00 UTC for a few minutes; only one job runs at a time.

**This repository holds no data.** The feeds are downloaded into memory and
posted on; no file is written here, no artifact is kept; the job logs print
filenames, byte counts and HTTP statuses only. The feeds themselves are public
regulatory records about firms (names, CRD numbers, regulatory assets under
management) published by the SEC for anyone to download. The server address
and token are repository secrets set by the owner (`PIPELINE_SERVER_URL`,
`PIPELINE_INGEST_TOKEN`); there is no pull-request trigger, so no third-party
code can run with them.

Why here: the SEC's web application firewall blocks the private server's
datacenter address, so the files have to be fetched from somewhere else; a
public repository's free Actions minutes are that somewhere else (the same
route as the owner's deed fetcher and Northdata relay).

How it behaves towards the SEC: `requests` with an honest User-Agent that
names the project and a contact address, two downloads a week plus up to eight
HEAD requests to find the published date. Nothing is disguised, no proxy.

Files:
- `.github/workflows/sec-adv-fetch.yml` — the schedule (Mondays 09:00 UTC) and the job.
- `.github/workflows/sec-adv-probe.yml` — a manual probe that only reports
  the published feed date and sizes (no server, no secrets).
- `sec_adv_fetch.py` — the fetch script (probe date → download → post).

The canonical copy of these files lives in the owner's private repository
(`helper/ii-sec-adv-fetch/`); this repository is a mirror pushed by a sync
script that stamps the line below. GitHub disables scheduled workflows on
public repositories after 60 days without a commit; the monthly sync keeps the
schedule alive.

Source commit: 3b871d61 (investment-intelligence, helper/ii-sec-adv-fetch/)
