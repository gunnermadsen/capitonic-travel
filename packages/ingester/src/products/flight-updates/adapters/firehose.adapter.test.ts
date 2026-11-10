import assert from 'node:assert/strict';
import test from 'node:test';
import { FirehoseFrames } from './firehose.adapter.js';
import { comparePitr } from './firehose-normalization.js';
test('stream framing handles multiple records and arbitrarily split UTF-8 without loss', () => {
  const frames = new FirehoseFrames(),
    bytes = Buffer.from('{"value":"é"}\n{"type":"keepalive"}\n'),
    lines: string[] = [];
  for (const byte of bytes) lines.push(...frames.push(Buffer.from([byte])));
  assert.deepEqual(lines, ['{"value":"é"}', '{"type":"keepalive"}']);
  frames.finish();
});
test('oversized and truncated frames fail rather than silently skipping data', () => {
  const frames = new FirehoseFrames();
  assert.throws(() => frames.push(Buffer.alloc(1048577, 65)));
  const partial = new FirehoseFrames();
  partial.push(Buffer.from('{'));
  assert.throws(() => partial.finish());
});
test('recovery point ordering retains subsecond precision', () => {
  assert.equal(comparePitr('1794197579.000000001', '1794197579.000000002'), -1);
  assert.equal(comparePitr('1.1', '1.100'), 0);
});
