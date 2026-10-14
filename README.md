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
| `CONCURRENCY` | `20` | Global concurrent checks, capped at 50 |
| `HOST_CONCURRENCY` | `2` | Maximum simultaneous checks against one hostname, capped at 10 |
| `TIMEOUT_MS` | `10000` | Timeout for each stream |
| `SHARD_INDEX` | `0` | Zero-based deterministic shard index |
| `SHARD_COUNT` | `1` | Number of deterministic shards |

Scheduled shards are assigned by stream hostname. This keeps endpoints on the same host on one runner and, together with `HOST_CONCURRENCY`, avoids multiplying connection pressure against a broadcaster or CDN.

Reports are written to `reports/<cc>.json`, `reports/all.json`, or a shard-specific filename such as `reports/all-shard-01-of-08.json`.

Each report includes online/offline counts, online rate, successful-check average latency, p50/p95 latency, average attempt duration, HTTP status counts, failure-reason counts, and per-stream results.

Failure reasons currently include `http_error`, `empty_response`, `timeout`, `dns_error`, `tls_error`, `connection_refused`, `connection_reset`, and `network_error`.

## GitHub Actions

The workflow has three modes:

- pushes that change the checker run a small JP smoke check;
- the daily scheduled run, after the database/streams/API update window, checks the full dataset across eight deterministic parallel shards and produces an aggregate `summary.json` artifact;
- manual runs can target a country and set the limit and concurrency.

The aggregate step rejects missing shards, mixed stream snapshots, and incomplete coverage instead of publishing a partial summary. Reports are uploaded as workflow artifacts rather than committed because availability data is time-sensitive and would otherwise create noisy repository history.

## Scope

A successful check means that the endpoint returned bytes during that request from the GitHub Actions runner region. It does not establish global availability, broadcast rights, long-term reliability, or permission to record or redistribute a stream.
