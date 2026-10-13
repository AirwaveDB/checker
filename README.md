# AirwaveDB Checker

Independent availability checks for stream endpoints published by [`AirwaveDB/streams`](https://github.com/AirwaveDB/streams).

The checker reads the canonical JSON stream data, opens each selected endpoint directly, follows redirects, requests only a small byte range, and records the HTTP result, content type, final URL, first-byte latency, and failure reason.

## Run

Node.js 22 or newer is required.

```bash
COUNTRY=JP node scripts/check.mjs
```

Environment variables:

| Variable | Default | Description |
| --- | ---: | --- |
| `COUNTRY` | all | ISO 3166-1 alpha-2 country code |
| `LIMIT` | `0` | Maximum number of selected streams; `0` means no limit |
| `CONCURRENCY` | `20` | Concurrent checks, capped at 50 |
| `TIMEOUT_MS` | `10000` | Timeout for each stream |
| `SHARD_INDEX` | `0` | Zero-based deterministic shard index |
| `SHARD_COUNT` | `1` | Number of deterministic shards |

Reports are written to `reports/<cc>.json`, `reports/all.json`, or a shard-specific filename such as `reports/all-shard-01-of-08.json`.

Each report includes aggregate online/offline counts, online rate, average first-byte latency, HTTP status counts, failure-reason counts, and the per-stream results.

Failure reasons currently include `http_error`, `empty_response`, `timeout`, `dns_error`, `tls_error`, `connection_refused`, `connection_reset`, and `network_error`.

## GitHub Actions

The workflow has three modes:

- pushes that change the checker run a small JP smoke check;
- the daily scheduled run, after the database/streams/API update window, checks the full dataset across eight deterministic parallel shards and produces an aggregate `summary.json` artifact;
- manual runs can target a country and set the limit and concurrency.

Shard reports and the aggregate summary are uploaded as workflow artifacts instead of being committed because availability data is time-sensitive and would otherwise create noisy repository history.

## Scope

A successful check means that the endpoint returned bytes during that request from the GitHub Actions runner region. It does not establish global availability, broadcast rights, long-term reliability, or permission to record or redistribute a stream.
