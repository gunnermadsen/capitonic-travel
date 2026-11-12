import { createFirehoseFrames } from './helpers.js';
import { comparePitr } from '../flight-updates.service.js';
import assert from 'node:assert/strict';
import test from 'node:test';
test('stream framing handles multiple records and arbitrarily split UTF-8 without loss', () => {
  const frames = createFirehoseFrames(),
    bytes = Buffer.from('{"value":"é"}\n{"type":"keepalive"}\n'),
    lines: string[] = [];
  for (const byte of bytes) lines.push(...frames.push(Buffer.from([byte])));
  assert.deepEqual(lines, ['{"value":"é"}', '{"type":"keepalive"}']);
  frames.finish();
});
test('oversized and truncated frames fail rather than silently skipping data', () => {
  const frames = createFirehoseFrames();
  assert.throws(() => frames.push(Buffer.alloc(1048577, 65)));
  const partial = createFirehoseFrames();
  partial.push(Buffer.from('{'));
  assert.throws(() => partial.finish());
});
test('recovery point ordering retains subsecond precision', () => {
  assert.equal(comparePitr('1794197579.000000001', '1794197579.000000002'), -1);
  assert.equal(comparePitr('1.1', '1.100'), 0);
});
