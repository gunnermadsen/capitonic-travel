import { readFile } from 'node:fs/promises';
try {
  const input: unknown = JSON.parse(
    await readFile('/tmp/ingester-health.json', 'utf8'),
  );
  if (
    typeof input !== 'object' ||
    input === null ||
    !('observedAt' in input) ||
    typeof input.observedAt !== 'string' ||
    !Number.isFinite(Date.parse(input.observedAt)) ||
    Date.now() - Date.parse(input.observedAt) > 5000
  )
    throw new Error('Stale health heartbeat');
  const mode = process.argv[2] ?? 'ready';
  if (mode === 'metrics') console.log(JSON.stringify(input));
  else if (mode === 'live' && 'live' in input && input.live === true)
    process.exitCode = 0;
  else if (mode === 'ready' && 'ready' in input && input.ready === true)
    process.exitCode = 0;
  else process.exitCode = 1;
} catch {
  process.exitCode = 1;
}
