import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseFlightUpdate } from '@airline/contracts';
import { aeroApiConfiguration } from '../aeroapi.configuration.js';
import {
  AeroApiAdapter,
  AeroApiHttpError,
} from '../adapters/aeroapi.adapter.js';
import {
  AeroApiCheckpointStore,
  type AeroApiPublication,
} from '../adapters/aeroapi-checkpoint.store.js';
import { normalizeAeroApi } from '../adapters/aeroapi-normalization.js';
import {
  parseAeroApiResponse,
  type AeroApiSchedule,
} from '../domain/aeroapi.contract.js';
import { IngestionStatus } from '../../../observability/ingestion-status.service.js';
import { AeroApiStrategy } from './aeroapi.strategy.js';
const now = new Date('2026-11-09T01:00:00.000Z');
const flight = {
  fa_flight_id: 'AAL1-123-airline-1',
  operator_icao: 'AAL',
  flight_number: '1',
  origin: { code_iata: 'JFK', timezone: 'America/New_York' },
  destination: { code_iata: 'LAX', timezone: 'America/Los_Angeles' },
  scheduled_out: '2026-11-09T00:00:00Z',
  scheduled_in: '2026-11-09T06:00:00Z',
  estimated_out: null,
  estimated_in: null,
  actual_out: null,
  actual_off: null,
  actual_on: null,
  actual_in: null,
  cancelled: false,
  diverted: false,
  status: 'Scheduled',
};
test('AeroAPI gate fields, provenance, service date and current-data guard satisfy canonical contract', () => {
  const e = normalizeAeroApi(
    { ...flight, actual_on: '2026-11-09T00:30:00Z' },
    now.toISOString(),
    1,
    now,
  );
  assert.ok(parseFlightUpdate(e).ok);
  assert.equal(e.service_date, '2026-11-08');
  assert.equal(e.flight_status, 'LANDED');
  assert.equal(e.actual_gate_arrival_at, null);
  assert.equal(e.source_timestamp_provenance, 'LOCALLY_OBSERVED');
  assert.equal(e.boarding_cutoff_at, null);
  assert.equal('simulation_run_id' in e, false);
  assert.throws(() =>
    normalizeAeroApi(
      {
        ...flight,
        scheduled_out: '2020-01-01T00:00:00Z',
        scheduled_in: '2020-01-01T06:00:00Z',
      },
      now.toISOString(),
      1,
      now,
    ),
  );
  assert.throws(() =>
    normalizeAeroApi({ ...flight, origin: null }, now.toISOString(), 1, now),
  );
  assert.equal(
    normalizeAeroApi({ ...flight, cancelled: true }, now.toISOString(), 1, now)
      .flight_status,
    'UNKNOWN',
  );
  assert.equal(
    normalizeAeroApi(
      { ...flight, cancelled: true, status: 'Cancelled' },
      now.toISOString(),
      1,
      now,
    ).flight_status,
    'CANCELLED',
  );
  assert.equal(
    normalizeAeroApi(
      { ...flight, scheduled_out: '2026-11-09T05:00:00Z' },
      now.toISOString(),
      2,
      now,
      e,
    ).service_date,
    e.service_date,
  );
});
test('HTTP adapter caps pages, uses exact configured key, never follows next or retries', async () => {
  const config = aeroApiConfiguration({ FLIGHTAWARE_API_KEY: 'test' }, '/tmp');
  let calls = 0;
  const adapter = new AeroApiAdapter(config, async (input, init) => {
    calls++;
    const url = new URL(String(input));
    assert.equal(
      url.pathname,
      `/aeroapi/airports/KJFK/flights/${calls === 1 ? 'scheduled_arrivals' : 'scheduled_departures'}`,
    );
    assert.equal(url.searchParams.get('type'), 'Airline');
    assert.equal(url.searchParams.get('max_pages'), '1');
    assert.match(
      url.searchParams.get('start') ?? '',
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/,
    );
    assert.match(
      url.searchParams.get('end') ?? '',
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/,
    );
    assert.equal(new Headers(init?.headers).get('x-apikey'), 'test');
    return new Response(
      JSON.stringify({
        [calls === 1 ? 'scheduled_arrivals' : 'scheduled_departures']: [flight],
        num_pages: 1,
        links: { next: '/another-page' },
      }),
    );
  });
  assert.equal(
    (
      await adapter.acquire(
        'scheduled_arrivals',
        now,
        new AbortController().signal,
      )
    ).length,
    1,
  );
  assert.equal(
    (
      await adapter.acquire(
        'scheduled_departures',
        now,
        new AbortController().signal,
      )
    ).length,
    1,
  );
  assert.equal(calls, 2);
  const limited = new AeroApiAdapter(config, async () => {
    calls++;
    return new Response('', { status: 429, headers: { 'retry-after': '600' } });
  });
  await assert.rejects(
    limited.acquire('scheduled_arrivals', now, new AbortController().signal),
    (e: unknown) => e instanceof AeroApiHttpError && e.retryAfterMs === 600000,
  );
  assert.equal(calls, 3);
});
test('durable polling, deduplication, lost-ack replay and changed snapshots', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'aeroapi-test-'));
  try {
    const config = aeroApiConfiguration(
        {
          FLIGHTAWARE_API_KEY: 'test',
          AEROAPI_AIRPORT: 'KJFK',
        },
        dir,
      ),
      store = new AeroApiCheckpointStore(dir);
    let clock = now,
      calls = 0,
      fail = true;
    let row = flight;
    const events: AeroApiPublication[] = [];
    const source = {
      acquire: async () => {
        calls++;
        return [row];
      },
    };
    const publisher = {
      publish: async (e: AeroApiPublication) => {
        events.push(e);
        if (fail) throw new Error('Lost acknowledgement');
      },
    };
    const make = () =>
      new AeroApiStrategy(
        config,
        source,
        store,
        publisher,
        new IngestionStatus(),
        () => clock,
      );
    let strategy = make();
    await strategy.initialize();
    await assert.rejects(strategy.tick(new AbortController().signal));
    assert.equal((await store.load()).requestsUsed, 1);
    strategy = make();
    await strategy.initialize();
    fail = false;
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 1);
    assert.deepEqual(events[0], events[1]);
    clock = new Date(clock.getTime() + config.intervalMs);
    await strategy.tick(new AbortController().signal);
    assert.equal(events.length, 2);
    row = { ...flight, status: 'Boarding' };
    clock = new Date(clock.getTime() + config.intervalMs);
    await strategy.tick(new AbortController().signal);
    row = flight;
    clock = new Date(clock.getTime() + config.intervalMs);
    await strategy.tick(new AbortController().signal);
    assert.notEqual(events[0]?.event_id, events[3]?.event_id);
    strategy = make();
    await strategy.initialize();
    clock = new Date(clock.getTime() + config.intervalMs);
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 5);
    assert.equal((await store.load()).requestsUsed, 5);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
test('HTTP failure counts attempts, Retry-After survives restart, authorization errors halt', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'aeroapi-test-'));
  try {
    const config = aeroApiConfiguration({ FLIGHTAWARE_API_KEY: 'test' }, dir),
      store = new AeroApiCheckpointStore(dir);
    let clock = now,
      calls = 0;
    const source = {
      acquire: async (): Promise<unknown[]> => {
        calls++;
        throw new AeroApiHttpError(calls === 1 ? 429 : 403, 900000);
      },
    };
    const make = () =>
      new AeroApiStrategy(
        config,
        source,
        store,
        { publish: async () => {} },
        new IngestionStatus(),
        () => clock,
      );
    let strategy = make();
    await strategy.initialize();
    await assert.rejects(strategy.tick(new AbortController().signal));
    strategy = make();
    await strategy.initialize();
    clock = new Date(now.getTime() + 300000);
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 1);
    clock = new Date(now.getTime() + 900000);
    await assert.rejects(strategy.tick(new AbortController().signal));
    strategy = make();
    await strategy.initialize();
    clock = new Date(now.getTime() + 9000000);
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 2);
    assert.equal((await store.load()).haltReason, 'HTTP_403');
    await writeFile(join(dir, 'aeroapi.json'), '{}');
    await assert.rejects(store.load());
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
test('ambiguous provider IDs are rejected rather than overwriting flight state', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'aeroapi-test-'));
  try {
    const config = aeroApiConfiguration({ FLIGHTAWARE_API_KEY: 'test' }, dir),
      store = new AeroApiCheckpointStore(dir),
      events: AeroApiPublication[] = [];
    const strategy = new AeroApiStrategy(
      config,
      { acquire: async () => [flight, { ...flight, diverted: true }] },
      store,
      {
        publish: async (e) => {
          events.push(e);
        },
      },
      new IngestionStatus(),
      () => now,
    );
    await strategy.initialize();
    await strategy.tick(new AbortController().signal);
    assert.equal(events.length, 2);
    assert.ok(events.every((e) => e.event_type === 'INGESTION_REJECTED'));
    assert.equal((await store.load()).flights.length, 0);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a reservation saved before a disk error cannot be reused by the running worker', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'aeroapi-test-'));
  try {
    class FaultStore extends AeroApiCheckpointStore {
      fail = false;
      override async save(
        state: import('../adapters/aeroapi-checkpoint.store.js').AeroApiState,
      ): Promise<void> {
        await super.save(state);
        if (this.fail) {
          this.fail = false;
          throw new Error('Directory sync failed');
        }
      }
    }
    const config = aeroApiConfiguration({ FLIGHTAWARE_API_KEY: 'test' }, dir),
      store = new FaultStore(dir);
    let calls = 0;
    const strategy = new AeroApiStrategy(
      config,
      {
        acquire: async () => {
          calls++;
          return [];
        },
      },
      store,
      { publish: async () => {} },
      new IngestionStatus(),
      () => now,
    );
    await strategy.initialize();
    store.fail = true;
    await assert.rejects(strategy.tick(new AbortController().signal));
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 0);
    assert.equal((await store.load()).requestsUsed, 1);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('polling continues beyond the former lifetime cap and respects interval after restart', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'aeroapi-continuous-'));
  try {
    const config = aeroApiConfiguration(
      { FLIGHTAWARE_API_KEY: 'test' },
      dir,
      'validate',
    );
    const store = new AeroApiCheckpointStore(dir);
    await store.save({ ...(await store.load()), requestsUsed: 1000 });
    let clock = now;
    let calls = 0;
    const schedules: AeroApiSchedule[] = [];
    const status = new IngestionStatus();
    const make = () =>
      new AeroApiStrategy(
        config,
        {
          acquire: async (schedule) => {
            schedules.push(schedule);
            calls++;
            return [flight];
          },
        },
        store,
        { publish: async () => {} },
        status,
        () => clock,
      );
    let strategy = make();
    await strategy.initialize();
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 1);
    assert.equal(status.validated, 1);
    assert.equal(status.published, 0);
    assert.equal(status.lastPublishedAt, null);
    strategy = make();
    await strategy.initialize();
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 1);
    clock = new Date(now.getTime() + config.intervalMs);
    await strategy.tick(new AbortController().signal);
    assert.equal(calls, 2);
    assert.deepEqual(schedules, ['scheduled_arrivals', 'scheduled_departures']);
    assert.equal((await store.load()).requestsUsed, 1002);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('airport response boundaries and long-haul arrivals preserve the canonical contract', () => {
  assert.deepEqual(
    parseAeroApiResponse(
      { scheduled_arrivals: [], num_pages: 1 },
      'scheduled_arrivals',
    ),
    [],
  );
  assert.throws(() =>
    parseAeroApiResponse(
      { flights: [flight], num_pages: 1 },
      'scheduled_arrivals',
    ),
  );
  assert.throws(() =>
    parseAeroApiResponse(
      { scheduled_departures: Array(16).fill(flight), num_pages: 1 },
      'scheduled_departures',
    ),
  );
  assert.throws(() =>
    parseAeroApiResponse(
      { scheduled_arrivals: [flight], num_pages: 2 },
      'scheduled_arrivals',
    ),
  );
  const event = normalizeAeroApi(
    {
      ...flight,
      scheduled_out: '2026-11-08T05:00:00Z',
      destination: { code_iata: 'JFK', timezone: 'America/New_York' },
    },
    now.toISOString(),
    1,
    now,
  );
  assert.ok(parseFlightUpdate(event).ok);
  assert.equal(event.destination, 'JFK');
  assert.throws(() =>
    aeroApiConfiguration(
      { FLIGHTAWARE_API_KEY: 'test', AEROAPI_AIRPORT: '../bad' },
      '/tmp',
    ),
  );
});
