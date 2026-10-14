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

function percentile(sorted, value) {
  if (!sorted.length) return 0;
  return sorted[Math.max(0, Math.ceil(value * sorted.length) - 1)];
}

async function main() {
  const files = (await readdir(REPORTS))
    .filter((name) => name.endsWith('.json'))
    .sort();

  if (!files.length) throw new Error('No checker reports found');

  const reports = [];
  for (const file of files) reports.push(JSON.parse(await readFile(join(REPORTS, file), 'utf8')));

  const expectedShards = reports[0]?.shard?.count;
  if (!Number.isInteger(expectedShards) || expectedShards < 1) throw new Error('Reports do not contain a valid shard count');
  if (reports.some((report) => report.shard?.count !== expectedShards)) throw new Error('Shard reports disagree about shard count');

  const shardIndexes = reports.map((report) => report.shard?.index).sort((a, b) => a - b);
  const expectedIndexes = Array.from({ length: expectedShards }, (_, index) => index);
  if (reports.length !== expectedShards || shardIndexes.some((value, index) => value !== expectedIndexes[index])) {
    throw new Error(`Incomplete shard set: got [${shardIndexes.join(', ')}], expected [${expectedIndexes.join(', ')}]`);
  }

  const sourceCounts = new Set(reports.map((report) => Number(report.sourceCount || 0)));
  if (sourceCounts.size !== 1) throw new Error('Shard reports disagree about sourceCount');
  const sourceCount = [...sourceCounts][0];

  const streamsGeneratedAtValues = new Set(reports.map((report) => report.streamsGeneratedAt || null));
  if (streamsGeneratedAtValues.size !== 1) throw new Error('Shard reports were produced from different stream snapshots');
  const streamsGeneratedAt = [...streamsGeneratedAtValues][0];

  let checked = 0;
  let online = 0;
  let attemptDurationTotal = 0;
  const successfulLatencies = [];
  const reasons = {};
  const httpStatuses = {};

  for (const report of reports) {
    checked += Number(report.checked || 0);
    online += Number(report.online || 0);
    addCounts(reasons, report.reasons);
    addCounts(httpStatuses, report.httpStatuses);
    for (const result of report.results || []) {
      attemptDurationTotal += Number(result.latencyMs || 0);
      if (result.ok) successfulLatencies.push(Number(result.latencyMs || 0));
    }
  }

  if (checked !== sourceCount) throw new Error(`Shard coverage incomplete: checked ${checked}, sourceCount ${sourceCount}`);
  if (successfulLatencies.length !== online) throw new Error(`Online result mismatch: ${successfulLatencies.length} successful results != ${online}`);

  successfulLatencies.sort((a, b) => a - b);
  const successfulLatencyTotal = successfulLatencies.reduce((sum, value) => sum + value, 0);

  const summary = {
    generatedAt: new Date().toISOString(),
    schemaVersion: 1,
    source: 'AirwaveDB/checker',
    streamsGeneratedAt,
    sourceCount,
    expectedShards,
    completedShards: reports.length,
    shardIndexes,
    checked,
    online,
    offline: checked - online,
    onlineRate: checked ? Number((online / checked).toFixed(4)) : 0,
    averageLatencyMs: online ? Math.round(successfulLatencyTotal / online) : 0,
    p50LatencyMs: percentile(successfulLatencies, 0.50),
    p95LatencyMs: percentile(successfulLatencies, 0.95),
    averageAttemptDurationMs: checked ? Math.round(attemptDurationTotal / checked) : 0,
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
