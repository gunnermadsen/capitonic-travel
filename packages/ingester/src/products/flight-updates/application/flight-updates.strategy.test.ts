import 'reflect-metadata';
import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseFlightUpdate, parseIngestionRejection } from '@airline/contracts';
import { configuration } from '../../../configuration/ingester.configuration.js';
import { FileCheckpointStore } from '../adapters/file-checkpoint.store.js';
import { IngestionStatus } from '../../../observability/ingestion-status.service.js';
import { SimulatorAdapter } from '../adapters/simulator.adapter.js';
import type {
  CheckpointStore,
  EventPublisher,
  FlightProvider,
  IngestionState,
  Publication,
} from '../domain/ingestion.ports.js';
import { FlightUpdatesStrategy } from './flight-updates.strategy.js';
const start = '2026-11-08T12:00:00.000Z',
  runId = '00000000-0000-4000-8000-000000000111';
const initial = (): IngestionState => ({
  version: 1,
  runId,
  startAt: start,
  nextSequence: 0,
  pending: null,
  lastPublishedAt: null,
  lastSourceEventTime: null,
});
class MemoryCheckpoint implements CheckpointStore {
  state = initial();
  failCommit = false;
  async load() {
    return structuredClone(this.state);
  }
  async save(state: IngestionState) {
    if (this.failCommit && state.pending === null)
      throw new Error('Disk unavailable');
    this.state = structuredClone(state);
  }
}
class Publisher implements EventPublisher {
  events: Publication[] = [];
  fail = false;
  async publish(event: Publication) {
    this.events.push(structuredClone(event));
    if (this.fail) throw new Error('Acknowledgement unavailable');
  }
}
const engine = (
  store: CheckpointStore,
  publisher: EventPublisher,
  provider: FlightProvider = new SimulatorAdapter(),
) =>
  new FlightUpdatesStrategy(
    provider,
    store,
    publisher,
    new IngestionStatus(),
    () => new Date(start),
  );
const signal = () => new AbortController().signal;
test('six deterministic snapshots retain flight identities, revisions, timing and provenance', async () => {
  const adapter = new SimulatorAdapter();
  const snapshots = [];
  for (let sequence = 0; sequence < 6; sequence++) {
    const context = { runId, startAt: start, sequence, observedAt: start };
    const raw = await adapter.acquire(context, signal());
    const value = adapter.normalize(raw, context);
    assert.equal(parseFlightUpdate(value).ok, true);
    assert.deepEqual(adapter.normalize(raw, context), value);
    snapshots.push(value);
  }
  assert.equal(
    snapshots[0]?.flight_instance_id,
    snapshots[2]?.flight_instance_id,
  );
  assert.equal(snapshots[2]?.source_revision, 1);
  assert.equal(snapshots[4]?.flight_status, 'CANCELLED');
  assert.equal(snapshots[3]?.boarding_cutoff_provenance, 'INFERRED_POLICY');
});
test('lost acknowledgements replay the same durable event after restart and advance only after success', async () => {
  const store = new MemoryCheckpoint(),
    publisher = new Publisher();
  publisher.fail = true;
  const first = engine(store, publisher);
  await first.initialize();
  await assert.rejects(first.tick(signal()));
  assert.equal(store.state.nextSequence, 0);
  assert.ok(store.state.pending);
  const pending = structuredClone(store.state.pending);
  publisher.fail = false;
  const restarted = engine(store, publisher);
  await restarted.initialize();
  await restarted.tick(signal());
  assert.deepEqual(publisher.events, [pending, pending]);
  assert.equal(store.state.nextSequence, 1);
  assert.equal(store.state.pending, null);
});
test('post-ack checkpoint failure preserves event identity for the next process', async () => {
  const store = new MemoryCheckpoint(),
    publisher = new Publisher();
  const first = engine(store, publisher);
  await first.initialize();
  store.failCommit = true;
  await assert.rejects(first.tick(signal()), /Disk/);
  assert.equal(store.state.nextSequence, 0);
  store.failCommit = false;
  const restarted = engine(store, publisher);
  await restarted.initialize();
  await restarted.tick(signal());
  assert.deepEqual(publisher.events[0], publisher.events[1]);
});
test('invalid snapshots produce safe validated rejection events whose acknowledgements gate progress', async () => {
  const provider: FlightProvider = {
    async acquire() {
      return Buffer.from('sensitive invalid payload');
    },
    normalize() {
      return { invalid: true };
    },
  };
  const store = new MemoryCheckpoint(),
    publisher = new Publisher();
  publisher.fail = true;
  const worker = engine(store, publisher, provider);
  await worker.initialize();
  await assert.rejects(worker.tick(signal()));
  assert.equal(store.state.nextSequence, 0);
  const rejected = publisher.events[0];
  assert.ok(rejected);
  assert.equal(parseIngestionRejection(rejected).ok, true);
  assert.ok(!JSON.stringify(rejected).includes('sensitive'));
  publisher.fail = false;
  await worker.tick(signal());
  assert.equal(store.state.nextSequence, 1);
  assert.deepEqual(publisher.events[0], publisher.events[1]);
});
test('acquisition interruption advances nothing, including providers that ignore cancellation', async () => {
  const provider: FlightProvider = {
    acquire() {
      return new Promise(() => {});
    },
    normalize() {
      return null;
    },
  };
  const store = new MemoryCheckpoint(),
    publisher = new Publisher(),
    controller = new AbortController(),
    worker = engine(store, publisher, provider);
  await worker.initialize();
  const pending = worker.tick(controller.signal);
  controller.abort();
  await assert.rejects(pending, /interrupted/);
  assert.equal(store.state.nextSequence, 0);
  assert.equal(publisher.events.length, 0);
});
test('atomic file checkpoint survives reconstruction and refuses corrupt or mismatched state', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'airline-ingester-'));
  try {
    const config = configuration({
      INGESTER_STATE_DIR: directory,
      IDENTITY_DIR: directory,
      SIMULATION_RUN_ID: runId,
      SIMULATION_START_AT: start,
    });
    const store = new FileCheckpointStore(config),
      publisher = new Publisher();
    publisher.fail = true;
    const worker = engine(store, publisher);
    await worker.initialize();
    await assert.rejects(worker.tick(signal()));
    const restored = await new FileCheckpointStore(config).load();
    assert.equal(restored.nextSequence, 0);
    assert.deepEqual(restored.pending, publisher.events[0]);
    await assert.rejects(
      new FileCheckpointStore({
        ...config,
        runId: '00000000-0000-4000-8000-000000000222',
      }).load(),
      /differs/,
    );
    assert.ok(
      (await readFile(join(directory, 'flight-updates.json'), 'utf8')).includes(
        runId,
      ),
    );
    await writeFile(join(directory, 'flight-updates.json'), '{broken');
    await assert.rejects(store.load());
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
test('configuration fails closed for unsupported modes and invalid limits', () => {
  for (const env of [
    { INGESTER_MODE: 'PUBLIC' },
    { INGESTER_INTERVAL_MS: '0' },
    { INGESTER_STATE_DIR: 'relative' },
    { SIMULATION_START_AT: 'invalid' },
    { SIMULATION_RUN_ID: 'invalid' },
  ])
    assert.throws(() => configuration(env));
});
