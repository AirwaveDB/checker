# AirwaveDB Checker

Independent availability checks for stream endpoints published by [`AirwaveDB/streams`](https://github.com/AirwaveDB/streams).

The checker opens each selected stream directly, follows redirects, requests only a small byte range, and records the HTTP result, content type, final URL, first-byte latency, and whether audio bytes were returned.

## Run

Node.js 22 or newer is required.

```bash
COUNTRY=JP node scripts/check.mjs
```

Environment variables:

| Variable | Default | Description |
| --- | ---: | --- |
| `COUNTRY` | all | ISO 3166-1 alpha-2 country code |
| `LIMIT` | `0` | Maximum number of streams; `0` means no limit |
| `CONCURRENCY` | `20` | Concurrent checks, capped at 50 |
| `TIMEOUT_MS` | `10000` | Timeout for each stream |

Reports are written to `reports/<cc>.json` or `reports/all.json`.

## GitHub Actions

The manual workflow accepts a country code and optional limit and uploads the generated report as a workflow artifact. Reports are not committed automatically because endpoint availability is time-sensitive and frequent checks would create noisy repository history.

## Scope

A successful check only means that the endpoint returned bytes during that request. It does not determine broadcast rights, geo-availability from other regions, long-term reliability, or whether a stream permits recording or redistribution.