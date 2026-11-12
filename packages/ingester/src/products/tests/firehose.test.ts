import { IngestionStatus } from '../../observability/ingestion-status.service.js';
import { configuration } from '../../configuration/ingester.configuration.js';
import { createFirehoseCheckpointStore } from './helpers.js';
import type { FirehoseState } from '../flight-updates.service.js';
import type { Publication } from '../flight-updates.service.js';
import { createFirehoseFlightUpdatesStrategy } from './helpers.js';
import 'reflect-metadata';
import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseFlightUpdate } from '@airline/contracts';
const base = {
  type: 'flifo',
  pitr: '1794197579',
  id: 'AFR23-1577943967-airline-0311',
  ident: 'AFR23',
  orig: 'KJFK',
  dest: 'LFPG',
  scheduled_out: '1578174000',
  scheduled_in: '1578199800',
  status: 'F',
};
const raw = (extra: Record<string, unknown> = {}) =>
  JSON.stringify({ ...base, ...extra });
class Store {
  state: FirehoseState = {
    version: 1,
    source: 'flightaware-firehose',
    airports: 'KJFK',
    pitr: null,
    lastObservedAt: null,
    flights: {},
    pending: null,
  };
  failCommit = false;
  async load() {
    return structuredClone(this.state);
  }
  async save(next: FirehoseState) {
    if (this.failCommit && !next.pending) throw new Error('Disk failure');
    this.state = structuredClone(next);
  }
}
class Publisher {
  events: Publication[] = [];
  fail = false;
  async publish(event: Publication) {
    this.events.push(structuredClone(event));
    if (this.fail) throw new Error('Acknowledgement lost');
  }
}
const strategy = (store: Store, publisher: Publisher) =>
  createFirehoseFlightUpdatesStrategy(
    store,
    publisher,
    new IngestionStatus(),
    () => new Date('2026-11-08T00:00:00.000Z'),
  );
test('Firehose full and partial updates satisfy immutable v1 without confusing runway and gate times', async () => {
  const store = new Store(),
    publisher = new Publisher(),
    engine = strategy(store, publisher);
  await engine.initialize();
  await engine.accept(
    raw({ estimated_on: '1578199000', predicted_in: '1578200000' }),
  );
  await engine.accept(
    JSON.stringify({
      type: 'flifo',
      id: base.id,
      pitr: '1794197580',
      estimated_in: '1578200400',
    }),
  );
  const [first, second] = publisher.events;
  assert.ok(first?.event_type === 'FLIGHT_UPDATED');
  assert.ok(second?.event_type === 'FLIGHT_UPDATED');
  assert.ok(parseFlightUpdate(second).ok);
  assert.equal(first.estimated_gate_arrival_at, null);
  assert.equal(second.estimated_gate_arrival_at, '2020-01-05T05:00:00.000Z');
  assert.equal(first.service_date, '2020-01-04');
  assert.equal(second.flight_instance_id, first.flight_instance_id);
  assert.equal(second.origin, 'JFK');
  assert.equal(second.destination, 'CDG');
  assert.equal(second.source_updated_at, null);
  assert.equal(second.source_timestamp_provenance, 'LOCALLY_OBSERVED');
  assert.equal(second.boarding_cutoff_provenance, 'UNKNOWN');
  assert.equal('simulation_run_id' in second, false);
  assert.ok(second.ingested_at > first.ingested_at);
  await engine.accept(
    JSON.stringify({
      type: 'flifo',
      id: base.id,
      pitr: '1794197581',
      scheduled_out: '1578300000',
      estimated_in: null,
    }),
  );
  const third = publisher.events[2];
  assert.ok(third?.event_type === 'FLIGHT_UPDATED');
  assert.equal(third.service_date, first.service_date);
  assert.equal(third.estimated_gate_arrival_at, null);
});
test('duplicates and older replay cannot regress state; distinct same-second updates survive', async () => {
  const store = new Store(),
    publisher = new Publisher(),
    engine = strategy(store, publisher);
  await engine.initialize();
  await engine.accept(raw());
  await engine.accept(raw());
  await engine.accept(raw({ pitr: '1794197578', estimated_in: '1578200400' }));
  assert.equal(publisher.events.length, 1);
  await engine.accept(raw({ estimated_in: '1578200400' }));
  assert.equal(publisher.events.length, 2);
  await engine.accept(raw({ pitr: '1794197580', estimated_in: '1578200400' }));
  assert.equal(publisher.events.length, 2);
  await engine.accept(
    JSON.stringify({ type: 'keepalive', pitr: '1794197581' }),
  );
  assert.equal(engine.recoveryPoint(), '1794197581');
});
test('lost acknowledgement and post-ack disk failure replay exact event before advancing recovery point', async () => {
  for (const kind of ['ack', 'disk']) {
    const store = new Store(),
      publisher = new Publisher(),
      first = strategy(store, publisher);
    await first.initialize();
    publisher.fail = kind === 'ack';
    store.failCommit = kind === 'disk';
    await assert.rejects(first.accept(raw()));
    assert.equal(store.state.pitr, null);
    assert.ok(store.state.pending);
    publisher.fail = false;
    store.failCommit = false;
    const restarted = strategy(store, publisher);
    await restarted.initialize();
    assert.deepEqual(publisher.events[0], publisher.events[1]);
    assert.equal(restarted.recoveryPoint(), base.pitr);
    await restarted.accept(raw());
    assert.equal(publisher.events.length, 2);
  }
});
test('incomplete flight state is retained until normalization succeeds and invalid data is redacted', async () => {
  const store = new Store(),
    publisher = new Publisher(),
    engine = strategy(store, publisher);
  await engine.initialize();
  await engine.accept(raw({ scheduled_out: null }));
  assert.equal(publisher.events[0]?.event_type, 'INGESTION_REJECTED');
  await engine.accept(
    JSON.stringify({
      type: 'flifo',
      id: base.id,
      pitr: '1794197580',
      scheduled_out: base.scheduled_out,
    }),
  );
  assert.equal(publisher.events[1]?.event_type, 'FLIGHT_UPDATED');
  await engine.accept(raw({ pitr: '1794197581', orig: 'SECRET_UNKNOWN' }));
  assert.equal(publisher.events[2]?.event_type, 'INGESTION_REJECTED');
  assert.ok(!JSON.stringify(publisher.events[2]).includes('SECRET_UNKNOWN'));
});
test('confirmed cancellation, uncancel, runway arrival and gate arrival have distinct semantics', async () => {
  const store = new Store(),
    publisher = new Publisher(),
    engine = strategy(store, publisher);
  await engine.initialize();
  await engine.accept(raw());
  await engine.accept(
    JSON.stringify({
      type: 'cancellation',
      id: base.id,
      pitr: '1794197580',
      trueCancel: 1,
    }),
  );
  await engine.accept(
    JSON.stringify({
      type: 'cancellation',
      id: base.id,
      pitr: '1794197581',
      uncancel: 1,
    }),
  );
  await engine.accept(
    JSON.stringify({
      type: 'arrival',
      id: base.id,
      pitr: '1794197582',
      timeType: 'actual',
      aat: '1578199800',
    }),
  );
  await engine.accept(
    JSON.stringify({
      type: 'flifo',
      id: base.id,
      pitr: '1794197583',
      actual_in: '1578200400',
    }),
  );
  assert.deepEqual(
    publisher.events.map((e) =>
      e.event_type === 'FLIGHT_UPDATED' ? e.flight_status : e.event_type,
    ),
    ['SCHEDULED', 'CANCELLED', 'SCHEDULED', 'LANDED', 'ARRIVED'],
  );
});
test('public checkpoint survives reconstruction and refuses mismatched subscriptions and corrupt state', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'firehose-checkpoint-'));
  try {
    const store = createFirehoseCheckpointStore(dir, 'KJFK');
    const state = await store.load();
    await store.save({ ...state, pitr: base.pitr });
    assert.equal(
      (await createFirehoseCheckpointStore(dir, 'KJFK').load()).pitr,
      base.pitr,
    );
    await assert.rejects(createFirehoseCheckpointStore(dir, 'KLAX').load());
    await writeFile(join(dir, 'flight-updates-firehose.json'), '{}');
    await assert.rejects(store.load());
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
test('exact .env secret names are required and protocol injection is rejected', () => {
  assert.equal(
    configuration({
      INGESTER_MODE: 'PUBLIC',
      FIREHOSE_USERNAME: 'user',
      FIREHOSE_API_KEY: 'key',
    }).FIREHOSE_API_KEY,
    'key',
  );
  assert.throws(() =>
    configuration({
      INGESTER_MODE: 'PUBLIC',
      FLIGHTAWARE_FIREHOSE_USERNAME: 'user',
      FLIGHTAWARE_FIREHOSE_API_KEY: 'key',
    }),
  );
  assert.throws(() =>
    configuration({
      INGESTER_MODE: 'PUBLIC',
      FIREHOSE_USERNAME: 'user\nlive',
      FIREHOSE_API_KEY: 'key',
    }),
  );
});

test('rejected updates retain replay identity after restart, including malformed fields', async () => {
  const store = new Store(),
    publisher = new Publisher(),
    engine = strategy(store, publisher);
  await engine.initialize();
  const invalid = raw({ scheduled_out: 'invalid' });
  await engine.accept(invalid);
  const restarted = strategy(store, publisher);
  await restarted.initialize();
  await restarted.accept(invalid);
  assert.equal(publisher.events.length, 1);
  assert.equal(publisher.events[0]?.event_type, 'INGESTION_REJECTED');
});

test('protocol errors and missing provider identity leave recovery progress unchanged', async () => {
  const store = new Store(),
    publisher = new Publisher(),
    engine = strategy(store, publisher);
  await engine.initialize();
  await assert.rejects(
    engine.accept(
      JSON.stringify({
        type: 'error',
        error_msg:
          'Maximum simultaneous connection limit for account exceeded.',
      }),
    ),
    { name: 'FirehoseConnectionLimitError' },
  );
  await assert.rejects(
    engine.accept(JSON.stringify({ type: 'flifo', pitr: base.pitr })),
  );
  await assert.rejects(engine.accept('{broken'));
  assert.equal(store.state.pitr, null);
  assert.equal(publisher.events.length, 0);
});
