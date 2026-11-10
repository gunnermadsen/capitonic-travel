import { parseFlightUpdate, type FlightUpdateV1 } from '@airline/contracts';
import airports from './reference/airports.json' with { type: 'json' };
import { eventIdentity } from '../domain/event-identity.js';
export function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
export function recoveryPoint(value: unknown): string {
  if (typeof value !== 'string' || !/^\d{1,12}(?:\.\d{1,9})?$/.test(value))
    throw new Error('Invalid Firehose recovery point');
  return value;
}
export function comparePitr(a: string, b: string): number {
  const parts = (s: string) => {
    const [whole = '0', fraction = ''] = s.split('.');
    return BigInt(whole) * 1000000000n + BigInt(fraction.padEnd(9, '0'));
  };
  return parts(a) < parts(b) ? -1 : parts(a) > parts(b) ? 1 : 0;
}
export interface FlightState {
  fields: Record<string, string | number | null>;
  serviceDate: string | null;
  pitr: string;
  hashes: string[];
  snapshot: FlightUpdateV1 | null;
}
const fields = [
  'ident',
  'orig',
  'dest',
  'scheduled_out',
  'scheduled_in',
  'estimated_out',
  'estimated_in',
  'actual_in',
  'actual_out',
  'actual_off',
  'actual_on',
  'status',
  'trueCancel',
  'uncancel',
];
const timestamps = new Set([
  'scheduled_out',
  'scheduled_in',
  'estimated_out',
  'estimated_in',
  'actual_in',
  'actual_out',
  'actual_off',
  'actual_on',
]);
export function instant(value: unknown): string | null {
  if (value === null || value === undefined || value === '') return null;
  if (
    (typeof value !== 'string' && typeof value !== 'number') ||
    !/^\d+(?:\.\d+)?$/.test(String(value))
  )
    throw new Error('Invalid Firehose time');
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 1 || seconds > 253402300799)
    throw new Error('Invalid Firehose time');
  return new Date(Math.floor(seconds * 1000)).toISOString();
}
export function mergeFlight(
  message: Record<string, unknown>,
  previous: FlightState | undefined,
  pitr: string,
  hash: string,
): FlightState {
  const merged = { ...previous?.fields };
  for (const key of fields) {
    if (!Object.hasOwn(message, key)) continue;
    const value = message[key];
    if (
      value !== null &&
      typeof value !== 'number' &&
      typeof value !== 'string'
    )
      throw new Error('Invalid Firehose field');
    if (timestamps.has(key)) instant(value);
    merged[key] = value;
  }
  if (message.type === 'departure' && message.adt !== undefined) {
    instant(message.adt);
    if (typeof message.adt === 'string' || typeof message.adt === 'number')
      merged.actual_off = message.adt;
  }
  if (
    message.type === 'arrival' &&
    message.timeType === 'actual' &&
    message.aat !== undefined
  ) {
    instant(message.aat);
    if (typeof message.aat === 'string' || typeof message.aat === 'number')
      merged.actual_on = message.aat;
  }
  if (message.type === 'cancellation') merged.status = 'X';
  if (message.uncancel === 1 || message.uncancel === '1') {
    merged.status = 'S';
    merged.trueCancel = null;
  }
  return {
    fields: merged,
    serviceDate: previous?.serviceDate ?? null,
    pitr,
    hashes:
      previous && comparePitr(previous.pitr, pitr) === 0
        ? [...previous.hashes, hash]
        : [hash],
    snapshot: previous?.snapshot ?? null,
  };
}
function airport(value: unknown): { iata: string; timezone: string } {
  const table: unknown = airports;
  if (typeof value !== 'string' || !record(table))
    throw new Error('Missing airport');
  const entry = table[value];
  if (
    !record(entry) ||
    typeof entry.iata !== 'string' ||
    typeof entry.timezone !== 'string'
  )
    throw new Error('Unknown airport');
  return { iata: entry.iata, timezone: entry.timezone };
}
export function normalizeFirehose(
  id: string,
  state: FlightState,
  hash: string,
  observedAt: string,
): FlightUpdateV1 {
  const f = state.fields,
    origin = airport(f.orig),
    destination = airport(f.dest);
  const ident =
    typeof f.ident === 'string'
      ? /^([A-Z]{3})([0-9]{1,4}[A-Z]?)$/.exec(f.ident)
      : null;
  if (!ident?.[1] || !ident[2])
    throw new Error('Missing operating flight identity');
  const scheduledOut = instant(f.scheduled_out);
  if (!state.serviceDate && !scheduledOut)
    throw new Error('Scheduled local service date unavailable');
  const dateParts = new Intl.DateTimeFormat('en-US', {
    timeZone: origin.timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date(scheduledOut ?? observedAt));
  const part = (key: string) => dateParts.find((p) => p.type === key)?.value;
  const serviceDate =
    state.serviceDate ?? `${part('year')}-${part('month')}-${part('day')}`;
  const actualIn = instant(f.actual_in),
    estimatedOut = instant(f.estimated_out);
  let status: FlightUpdateV1['flight_status'] = 'UNKNOWN';
  if (f.status === 'X')
    status =
      f.trueCancel === 1 || f.trueCancel === '1' ? 'CANCELLED' : 'UNKNOWN';
  else if (actualIn) status = 'ARRIVED';
  else if (instant(f.actual_on)) status = 'LANDED';
  else if (instant(f.actual_off)) status = 'IN_AIR';
  else if (instant(f.actual_out)) status = 'DEPARTED';
  else if (estimatedOut && scheduledOut && estimatedOut > scheduledOut)
    status = 'DELAYED';
  else if (f.status === 'S' || f.status === 'F') status = 'SCHEDULED';
  const event: FlightUpdateV1 = {
    event_id: eventIdentity(
      'flightaware-firehose',
      `${id}:${state.pitr}:${hash}`,
    ),
    schema_version: '1.0.0',
    event_type: 'FLIGHT_UPDATED',
    event_time: observedAt,
    ingested_at: observedAt,
    source: 'flightaware-firehose',
    operating_mode: 'PUBLIC',
    flight_instance_id: eventIdentity('flightaware-firehose', id),
    provider_flight_id: id,
    operating_carrier: ident[1],
    flight_number: ident[2],
    service_date: serviceDate,
    origin: origin.iata,
    destination: destination.iata,
    origin_timezone: origin.timezone,
    destination_timezone: destination.timezone,
    flight_status: status,
    scheduled_gate_arrival_at: instant(f.scheduled_in),
    estimated_gate_arrival_at: instant(f.estimated_in),
    actual_gate_arrival_at: actualIn,
    scheduled_gate_departure_at: scheduledOut,
    estimated_gate_departure_at: estimatedOut,
    boarding_cutoff_at: null,
    boarding_cutoff_provenance: 'UNKNOWN',
    boarding_cutoff_policy_version: null,
    source_revision: null,
    source_updated_at: null,
    source_timestamp_provenance: 'LOCALLY_OBSERVED',
  };
  const result = parseFlightUpdate(event);
  if (!result.ok)
    throw new Error('Normalized Firehose snapshot violates contract');
  return result.value;
}
