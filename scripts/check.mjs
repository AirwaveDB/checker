#!/usr/bin/env node
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const REPORTS = join(ROOT, 'reports');
const RAW = 'https://raw.githubusercontent.com/AirwaveDB/streams/main/data';
const USER_AGENT = 'AirwaveDB-checker/1.0 (+https://github.com/AirwaveDB/checker)';
const COUNTRY = String(process.env.COUNTRY || '').toUpperCase();
const LIMIT = Math.max(0, Number(process.env.LIMIT || 0));
const CONCURRENCY = Math.max(1, Math.min(50, Number(process.env.CONCURRENCY || 20)));
const TIMEOUT_MS = Math.max(2000, Number(process.env.TIMEOUT_MS || 10000));

function parseCsv(text) {
  const rows = [];
  let row = [], field = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') quoted = false;
      else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field.replace(/\r$/, '')); rows.push(row); row = []; field = ''; }
    else field += c;
  }
  if (field || row.length) { row.push(field); rows.push(row); }
  if (!rows.length) return [];
  const headers = rows.shift();
  return rows.filter((r) => r.some(Boolean)).map((r) => Object.fromEntries(headers.map((h, i) => [h, r[i] ?? ''])));
}

async function get(url, accept = 'application/json') {
  const res = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT, Accept: accept },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${url}`);
  return res;
}

async function check(row) {
  const started = Date.now();
  try {
    const res = await fetch(row.url, {
      method: 'GET',
      headers: { 'User-Agent': USER_AGENT, Accept: 'audio/*,*/*;q=0.5', Range: 'bytes=0-4095' },
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
    return {
      id: row.id,
      stationId: row.station_id,
      url: row.url,
      ok: res.ok && received > 0,
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
      stationId: row.station_id,
      url: row.url,
      ok: false,
      status: 0,
      contentType: '',
      finalUrl: '',
      bytes: 0,
      latencyMs: Date.now() - started,
      error: error.name === 'TimeoutError' ? 'timeout' : String(error.message || error),
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
      if ((i + 1) % 100 === 0) process.stdout.write(`checked ${i + 1}/${items.length}\n`);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

async function main() {
  const index = await (await get(`${RAW}/index.json`)).json();
  const countries = COUNTRY ? [COUNTRY] : index.countries.map((x) => x.code);
  let rows = [];
  for (const code of countries) {
    const text = await (await get(`${RAW}/streams/${code}.csv`, 'text/csv')).text();
    rows.push(...parseCsv(text));
  }
  if (LIMIT) rows = rows.slice(0, LIMIT);
  const results = await mapLimit(rows, CONCURRENCY, check);
  const online = results.filter((x) => x.ok).length;
  const report = {
    generatedAt: new Date().toISOString(),
    country: COUNTRY || null,
    checked: results.length,
    online,
    offline: results.length - online,
    results,
  };
  await mkdir(REPORTS, { recursive: true });
  const name = COUNTRY ? COUNTRY.toLowerCase() : 'all';
  await writeFile(join(REPORTS, `${name}.json`), JSON.stringify(report, null, 2) + '\n');
  process.stdout.write(`online ${online}/${results.length}\n`);
}

await main();
