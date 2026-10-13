#!/usr/bin/env node
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const REPORTS = join(ROOT, 'reports');
const RAW = 'https://raw.githubusercontent.com/AirwaveDB/streams/main/data';
const USER_AGENT = 'AirwaveDB-checker/1.1 (+https://github.com/AirwaveDB/checker)';

const COUNTRY = String(process.env.COUNTRY || '').trim().toUpperCase();
const LIMIT = Math.max(0, Number(process.env.LIMIT || 0));
const CONCURRENCY = Math.max(1, Math.min(50, Number(process.env.CONCURRENCY || 20)));
const TIMEOUT_MS = Math.max(2000, Number(process.env.TIMEOUT_MS || 10000));
const SHARD_COUNT = Math.max(1, Number(process.env.SHARD_COUNT || 1));
const SHARD_INDEX = Math.max(0, Number(process.env.SHARD_INDEX || 0));

if (!Number.isInteger(SHARD_COUNT) || !Number.isInteger(SHARD_INDEX) || SHARD_INDEX >= SHARD_COUNT) {
  throw new Error(`Invalid shard configuration: SHARD_INDEX=${SHARD_INDEX}, SHARD_COUNT=${SHARD_COUNT}`);
}

async function getJson(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${url}`);
  return res.json();
}

function shardFor(value, count) {
  // FNV-1a, kept deliberately simple and deterministic across runtimes.
  let hash = 0x811c9dc5;
  for (const c of String(value)) {
    hash ^= c.charCodeAt(0);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % count;
}

function classifyError(error) {
  if (error?.name === 'TimeoutError' || error?.name === 'AbortError') return 'timeout';

  const code = String(error?.cause?.code || '').toUpperCase();
  const message = String(error?.cause?.message || error?.message || '').toUpperCase();
  const value = `${code} ${message}`;

  if (value.includes('ENOTFOUND') || value.includes('EAI_AGAIN')) return 'dns_error';
  if (value.includes('ECONNREFUSED')) return 'connection_refused';
  if (value.includes('ECONNRESET')) return 'connection_reset';
  if (value.includes('CERT_') || value.includes('SSL') || value.includes('TLS')) return 'tls_error';
  return 'network_error';
}

async function check(row) {
  const started = Date.now();
  try {
    const res = await fetch(row.url, {
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        Accept: 'audio/*,application/vnd.apple.mpegurl,application/x-mpegURL,*/*;q=0.5',
        Range: 'bytes=0-4095',
      },
      redirect: 'follow',
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });

    let received = 0;
    if (res.body) {
      const reader = res.body.getReader();
      const first = await reader.read();
      received = first.value?.byteLength || 0;
      await reader.cancel();
    }

    const ok = res.ok && received > 0;
    const reason = !res.ok ? 'http_error' : received === 0 ? 'empty_response' : 'online';

    return {
      id: row.id,
      stationId: row.stationId,
      country: row.country || null,
      url: row.url,
      ok,
      reason,
      status: res.status,
      contentType: res.headers.get('content-type') || '',
      finalUrl: res.url,
      bytes: received,
      latencyMs: Date.now() - started,
      error: null,
    };
  } catch (error) {
    return {
      id: row.id,
      stationId: row.stationId,
      country: row.country || null,
      url: row.url,
      ok: false,
      reason: classifyError(error),
      status: 0,
      contentType: '',
      finalUrl: '',
      bytes: 0,
      latencyMs: Date.now() - started,
      error: String(error?.cause?.message || error?.message || error),
    };
  }
}

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      out[i] = await fn(items[i]);
      if ((i + 1) % 100 === 0 || i + 1 === items.length) {
        process.stdout.write(`checked ${i + 1}/${items.length}\n`);
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, Math.max(1, items.length)) }, worker));
  return out;
}

function summarize(results) {
  const reasons = {};
  const httpStatuses = {};
  let latencyTotal = 0;

  for (const result of results) {
    reasons[result.reason] = (reasons[result.reason] || 0) + 1;
    if (result.status) httpStatuses[result.status] = (httpStatuses[result.status] || 0) + 1;
    latencyTotal += result.latencyMs;
  }

  const online = results.filter((x) => x.ok).length;
  return {
    checked: results.length,
    online,
    offline: results.length - online,
    onlineRate: results.length ? Number((online / results.length).toFixed(4)) : 0,
    averageLatencyMs: results.length ? Math.round(latencyTotal / results.length) : 0,
    reasons,
    httpStatuses,
  };
}

async function main() {
  const index = await getJson(`${RAW}/index.json`);
  const countries = COUNTRY ? [COUNTRY] : index.countries.map((x) => x.code);

  let rows = [];
  for (const code of countries) {
    try {
      const streams = await getJson(`${RAW}/streams/${code}.json`);
      if (!Array.isArray(streams)) throw new Error(`Expected an array for ${code}`);
      rows.push(...streams);
    } catch (error) {
      if (COUNTRY) throw error;
      process.stderr.write(`skip ${code}: ${error.message}\n`);
    }
  }

  const sourceCount = rows.length;
  if (SHARD_COUNT > 1) {
    rows = rows.filter((row) => shardFor(row.id || row.url, SHARD_COUNT) === SHARD_INDEX);
  }
  if (LIMIT) rows = rows.slice(0, LIMIT);

  process.stdout.write(
    `checking ${rows.length}/${sourceCount} streams` +
      (COUNTRY ? ` country=${COUNTRY}` : '') +
      (SHARD_COUNT > 1 ? ` shard=${SHARD_INDEX + 1}/${SHARD_COUNT}` : '') +
      ` concurrency=${CONCURRENCY} timeout=${TIMEOUT_MS}ms\n`,
  );

  const results = await mapLimit(rows, CONCURRENCY, check);
  const summary = summarize(results);
  const report = {
    generatedAt: new Date().toISOString(),
    source: 'AirwaveDB/streams',
    country: COUNTRY || null,
    shard: SHARD_COUNT > 1 ? { index: SHARD_INDEX, count: SHARD_COUNT } : null,
    sourceCount,
    ...summary,
    results,
  };

  await mkdir(REPORTS, { recursive: true });
  const scope = COUNTRY ? COUNTRY.toLowerCase() : 'all';
  const shardSuffix = SHARD_COUNT > 1
    ? `-shard-${String(SHARD_INDEX + 1).padStart(2, '0')}-of-${String(SHARD_COUNT).padStart(2, '0')}`
    : '';
  const path = join(REPORTS, `${scope}${shardSuffix}.json`);
  await writeFile(path, JSON.stringify(report, null, 2) + '\n');

  process.stdout.write(`online ${summary.online}/${summary.checked} (${(summary.onlineRate * 100).toFixed(2)}%)\n`);
}

await main();
