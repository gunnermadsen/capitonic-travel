import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  parseEvent,
  parseEventJson,
  parseFlightUpdate,
  parseItineraryEvent,
  parseConnectionAssessment,
  parseIngestionRejection,
} from '../index.js';
function fixture(name: string): Record<string, unknown> {
  const value: unknown = JSON.parse(
    readFileSync(
      new URL(`../../fixtures/${name}.json`, import.meta.url),
      'utf8',
    ),
  );
  assert.ok(
    typeof value === 'object' && value !== null && !Array.isArray(value),
  );
  return Object.fromEntries(Object.entries(value));
}
const flight = (): Record<string, unknown> => fixture('flight-update');
const itinerary = (): Record<string, unknown> => fixture('itinerary-created');
const assessment = (): Record<string, unknown> =>
  fixture('connection-assessment');
const rejection = (): Record<string, unknown> => fixture('ingestion-rejection');
function valid(value: unknown): void {
  const result = parseEvent(value);
  assert.equal(result.ok, true, JSON.stringify(result));
}
function invalid(value: unknown): void {
  const result = parseEvent(value);
  assert.equal(result.ok, false);
  if (!result.ok)
    assert.ok(result.issues.length > 0 && result.issues.length <= 16);
}

test('all products validate through typed, generic, and serialized boundaries without mutation', () => {
  for (const [value, parse] of [
    [flight(), parseFlightUpdate],
    [itinerary(), parseItineraryEvent],
    [assessment(), parseConnectionAssessment],
    [rejection(), parseIngestionRejection],
  ] satisfies [
    Record<string, unknown>,
    (input: unknown) => { ok: boolean },
  ][]) {
    const original = JSON.stringify(value);
    assert.equal(parse(value).ok, true);
    valid(value);
    assert.deepEqual(parseEventJson(original), parseEvent(value));
    assert.equal(JSON.stringify(value), original);
  }
});
test('typed validation exposes usable generated fields after narrowing', () => {
  const result = parseFlightUpdate(flight());
  assert.ok(result.ok);
  const id: string = result.value.flight_instance_id;
  const revision: number | null = result.value.source_revision;
  assert.equal(id.length, 36);
  assert.equal(revision, null);
});
test('public and synthetic provenance, source revisions and explicit null snapshots', () => {
  const value = {
    ...flight(),
    operating_mode: 'PUBLIC',
    provider_flight_id: 'provider-123',
    source: 'provider',
    source_revision: 9007199254740991,
    source_timestamp_provenance: 'SOURCE_ISSUED',
    source_updated_at: '2026-11-08T12:00:00.000Z',
  };
  Reflect.deleteProperty(value, 'simulation_run_id');
  valid(value);
  invalid({
    ...value,
    simulation_run_id: '00000000-0000-4000-8000-000000000002',
  });
  const missingRun = flight();
  delete missingRun.simulation_run_id;
  invalid(missingRun);
  invalid({ ...value, source_updated_at: null });
  invalid({ ...value, event_time: '2026-11-08T12:01:00.000Z' });
  invalid({ ...flight(), source_updated_at: '2026-11-08T12:00:00.000Z' });
  valid({ ...flight(), source_revision: 0 });
  for (const revision of [-1, 1.5, 9007199254740992, '1'])
    invalid({ ...flight(), source_revision: revision });
  const missingTiming = flight();
  delete missingTiming.actual_gate_arrival_at;
  invalid(missingTiming);
  valid({
    ...flight(),
    scheduled_gate_arrival_at: null,
    scheduled_gate_departure_at: null,
  });
});
test('timestamps enforce UTC milliseconds, calendar validity, and local service date/timezone identity', () => {
  for (const instant of [
    '2026-11-08T12:00:00Z',
    '2026-11-08T12:00:00.000+00:00',
    '2026-11-08T12:00:00.0001Z',
    '2026-02-30T12:00:00.000Z',
    '2026-11-08T12:00:60.000Z',
    '0000-01-01T12:00:00.000Z',
  ])
    invalid({ ...flight(), scheduled_gate_arrival_at: instant });
  valid({ ...flight(), scheduled_gate_arrival_at: '2024-02-29T23:59:59.999Z' });
  invalid({ ...flight(), service_date: '2026-02-29' });
  invalid({ ...flight(), origin_timezone: 'Mars/Olympus' });
  valid({
    ...flight(),
    service_date: '2026-11-07',
    origin_timezone: 'Pacific/Honolulu',
  });
});
test('boarding cutoff provenance and policy version are mandatory and consistent', () => {
  const cutoff = '2026-11-08T12:30:00.000Z';
  valid({
    ...flight(),
    boarding_cutoff_at: cutoff,
    boarding_cutoff_provenance: 'SOURCE_PROVIDED',
  });
  valid({
    ...flight(),
    boarding_cutoff_at: cutoff,
    boarding_cutoff_provenance: 'INFERRED_POLICY',
    boarding_cutoff_policy_version: 'demo-v1',
  });
  invalid({ ...flight(), boarding_cutoff_at: cutoff });
  invalid({
    ...flight(),
    boarding_cutoff_provenance: 'INFERRED_POLICY',
    boarding_cutoff_at: cutoff,
  });
  invalid({
    ...flight(),
    boarding_cutoff_provenance: 'SOURCE_PROVIDED',
    boarding_cutoff_at: cutoff,
    boarding_cutoff_policy_version: 'demo-v1',
  });
});
test('itinerary creation, revision, cancellation and identity constraints', () => {
  for (const event_type of ['UPDATED', 'CANCELLED']) {
    valid({ ...itinerary(), event_type, itinerary_revision: 2 });
    invalid({ ...itinerary(), event_type });
  }
  invalid({ ...itinerary(), itinerary_revision: 2 });
  invalid({ ...itinerary(), minimum_transfer_minutes: 0 });
  invalid({ ...itinerary(), minimum_transfer_minutes: 1441 });
  invalid({
    ...itinerary(),
    outbound_flight_instance_id: itinerary().inbound_flight_instance_id,
  });
  invalid({ ...itinerary(), connection_id: 'not-a-uuid' });
});
test('assessment status, missing data, cancellation and traceable inputs', () => {
  valid({
    ...assessment(),
    status: 'AT_RISK',
    reason_code: 'TRANSFER_TIME_INSUFFICIENT',
    boarding_cutoff_at_used: '2026-11-08T18:10:00.000Z',
    available_transfer_minutes: 10,
  });
  valid({
    ...assessment(),
    status: 'DISRUPTED',
    reason_code: 'INBOUND_CANCELLED',
  });
  const unknown = {
    ...assessment(),
    status: 'UNKNOWN',
    reason_code: 'TIMING_UNAVAILABLE',
    arrival_at_used: null,
    arrival_basis: 'UNKNOWN',
    available_transfer_minutes: null,
  };
  valid(unknown);
  valid({ ...unknown, reason_code: 'ITINERARY_CANCELLED' });
  invalid({ ...unknown, arrival_basis: 'ACTUAL' });
  invalid({ ...unknown, available_transfer_minutes: 0 });
  invalid({ ...assessment(), reason_code: 'INBOUND_CANCELLED' });
  invalid({ ...assessment(), available_transfer_minutes: 61 });
  invalid({ ...assessment(), cutoff_policy_version: null });
  invalid({
    ...assessment(),
    trigger_event_id: '00000000-0000-4000-8000-000000000099',
  });
  valid({
    ...assessment(),
    status: 'AT_RISK',
    reason_code: 'TRANSFER_TIME_INSUFFICIENT',
    boarding_cutoff_at_used: '2026-11-08T17:59:59.999Z',
    available_transfer_minutes: -1,
  });
  invalid({ ...assessment(), revision: 0 });
});
test('rejections are bounded, typed, and cannot recursively reject rejections', () => {
  invalid({ ...rejection(), data_product: 'ingestion_rejections' });
  invalid({ ...rejection(), payload_sha256: 'a' });
  invalid({ ...rejection(), issues: [] });
  invalid({
    ...rejection(),
    issues: Array.from({ length: 17 }, () => ({
      path: '',
      code: 'ERROR',
      message: 'Invalid.',
    })),
  });
  invalid({ ...rejection(), raw_payload: 'secret' });
  const bad = parseEventJson('{secret');
  assert.equal(bad.ok, false);
  assert.ok(!JSON.stringify(bad).includes('secret'));
});
test('unknown properties, unsupported versions/types, wrong products and non-object inputs fail consistently', () => {
  for (const value of [
    null,
    [],
    true,
    42,
    '{}',
    {},
    { ...flight(), extra: true },
    { ...flight(), schema_version: '2.0.0' },
    { ...flight(), event_type: 'UNKNOWN_EVENT' },
    { ...flight(), correlation_id: null },
    { ...flight(), flight_status: 'invented' },
  ])
    invalid(value);
  assert.equal(parseFlightUpdate(itinerary()).ok, false);
  const bad = { ...flight(), source_revision: '123', extra: 'sensitive' };
  const before = JSON.stringify(bad);
  assert.deepEqual(parseEvent(bad), parseEventJson(before));
  assert.equal(JSON.stringify(bad), before);
  assert.ok(!JSON.stringify(parseEvent(bad)).includes('sensitive'));
});
