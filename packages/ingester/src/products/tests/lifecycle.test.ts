import assert from 'node:assert/strict';
import { test } from 'node:test';
import { configuration } from '../../configuration/ingester.configuration.js';
import { service } from './helpers.js';

for (const source of ['aeroapi', 'firehose', 'simulator']) {
  test(`${source}: starts only its source and shuts down its loop`, async (t) => {
    const config = configuration({
      INGESTER_PRODUCT: source === 'aeroapi' ? 'aeroapi' : 'flight-updates',
      INGESTER_MODE: source === 'simulator' ? 'SYNTHETIC' : 'PUBLIC',
      FIREHOSE_USERNAME: 'test',
      FIREHOSE_API_KEY: 'test',
    });
    const worker = service(config, undefined, undefined, undefined, undefined, {
      FLIGHTAWARE_API_KEY: 'test',
    });
    const aero = t.mock.method(worker, 'aeroInitialize', async () => {});
    const firehose = t.mock.method(
      worker,
      'firehoseInitialize',
      async () => {},
    );
    const sim = t.mock.method(worker, 'simInitialize', async () => {});
    const poll = t.mock.method(worker, 'aeroTick', async () => {});
    const tick = t.mock.method(worker, 'simTick', async () => {});
    t.mock.method(worker, 'firehoseFlush', async () => {});
    t.mock.method(worker, 'firehoseRecoveryPoint', () => null);
    const stream = t.mock.method(worker, 'firehoseStream', async function* () {
      yield 'keepalive';
    });
    t.mock.method(worker, 'firehoseAccept', async () => {});
    await worker.onApplicationBootstrap();
    await worker.beforeApplicationShutdown();
    assert.equal(aero.mock.callCount(), source === 'aeroapi' ? 1 : 0);
    assert.equal(firehose.mock.callCount(), source === 'firehose' ? 1 : 0);
    assert.equal(sim.mock.callCount(), source === 'simulator' ? 1 : 0);
    assert.equal(poll.mock.callCount(), source === 'aeroapi' ? 1 : 0);
    assert.equal(tick.mock.callCount(), source === 'simulator' ? 1 : 0);
    assert.equal(stream.mock.callCount(), source === 'firehose' ? 1 : 0);
    assert.equal(worker.status.stopping, true);
  });
}
