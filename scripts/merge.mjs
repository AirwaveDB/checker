#!/usr/bin/env node
import { readFile, readdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const REPORTS = join(ROOT, 'reports');
const OUTPUT = join(ROOT, 'summary.json');

function addCounts(target, source = {}) {
  for (const [key, value] of Object.entries(source)) {
    target[key] = (target[key] || 0) + Number(value || 0);
  }
}

async function main() {
  const files = (await readdir(REPORTS))
    .filter((name) => name.endsWith('.json'))
    .sort();

  if (!files.length) throw new Error('No checker reports found');

  const reports = [];
  for (const file of files) {
    reports.push(JSON.parse(await readFile(join(REPORTS, file), 'utf8')));
  }

  let checked = 0;
  let online = 0;
  let latencyWeighted = 0;
  const reasons = {};
  const httpStatuses = {};

  for (const report of reports) {
    checked += Number(report.checked || 0);
    online += Number(report.online || 0);
    latencyWeighted += Number(report.averageLatencyMs || 0) * Number(report.checked || 0);
    addCounts(reasons, report.reasons);
    addCounts(httpStatuses, report.httpStatuses);
  }

  const shardIndexes = reports
    .map((report) => report.shard?.index)
    .filter((value) => Number.isInteger(value))
    .sort((a, b) => a - b);
  const expectedShards = reports.find((report) => report.shard?.count)?.shard.count || reports.length;

  const summary = {
    generatedAt: new Date().toISOString(),
    source: 'AirwaveDB/checker',
    expectedShards,
    completedShards: reports.length,
    shardIndexes,
    checked,
    online,
    offline: checked - online,
    onlineRate: checked ? Number((online / checked).toFixed(4)) : 0,
    averageLatencyMs: checked ? Math.round(latencyWeighted / checked) : 0,
    reasons,
    httpStatuses,
  };

  await writeFile(OUTPUT, JSON.stringify(summary, null, 2) + '\n');
  process.stdout.write(
    `summary ${summary.completedShards}/${summary.expectedShards} shards, ` +
      `${online}/${checked} online (${(summary.onlineRate * 100).toFixed(2)}%)\n`,
  );
}

await main();
