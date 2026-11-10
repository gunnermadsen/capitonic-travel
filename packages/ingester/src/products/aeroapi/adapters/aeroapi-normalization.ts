import { createHash } from 'node:crypto';
import { parseFlightUpdate, type FlightUpdateV1 } from '@airline/contracts';
import { parseAeroApiFlight } from '../domain/aeroapi.contract.js';
export function aeroApiId(key: string): string {
  const hex = createHash('sha256')
    .update(`flightaware-aeroapi:${key}`)
    .digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
export function fingerprint(event: FlightUpdateV1): string {
  return JSON.stringify(
    Object.fromEntries(
      Object.entries(event).filter(
        ([key]) => !['event_id', 'event_time', 'ingested_at'].includes(key),
      ),
    ),
  );
}
export function normalizeAeroApi(
  raw: unknown,
  observedAt: string,
  requestNumber: number,
  now: Date,
  previous?: FlightUpdateV1,
): FlightUpdateV1 {
  const f = parseAeroApiFlight(raw),
    scheduled = Date.parse(f.scheduled_out);
  if (
    scheduled < now.getTime() - 18 * 3600000 ||
    scheduled > now.getTime() + 6 * 3600000
  )
    throw new Error('Flight is outside current observation window');
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: f.origin.timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date(f.scheduled_out));
  const part = (name: string) => parts.find((p) => p.type === name)?.value;
  let status: FlightUpdateV1['flight_status'] = 'SCHEDULED';
  if (f.cancelled) status = /cancel/i.test(f.status) ? 'CANCELLED' : 'UNKNOWN';
  else if (f.diverted) status = 'DIVERTED';
  else if (f.actual_in) status = 'ARRIVED';
  else if (f.actual_on) status = 'LANDED';
  else if (f.actual_off) status = 'IN_AIR';
  else if (f.actual_out) status = 'DEPARTED';
  else if (
    (f.estimated_out && f.estimated_out > f.scheduled_out) ||
    (f.estimated_in && f.scheduled_in && f.estimated_in > f.scheduled_in)
  )
    status = 'DELAYED';
  else if (/boarding/i.test(f.status)) status = 'BOARDING';
  const event: FlightUpdateV1 = {
    event_id: aeroApiId(`event:${requestNumber}:${f.fa_flight_id}`),
    schema_version: '1.0.0',
    event_type: 'FLIGHT_UPDATED',
    event_time: observedAt,
    ingested_at: observedAt,
    source: 'flightaware-aeroapi',
    operating_mode: 'PUBLIC',
    flight_instance_id: aeroApiId(`flight:${f.fa_flight_id}`),
    provider_flight_id: f.fa_flight_id,
    operating_carrier: f.operator_icao,
    flight_number: f.flight_number,
    service_date:
      previous?.service_date ??
      `${part('year')}-${part('month')}-${part('day')}`,
    origin: f.origin.code_iata,
    destination: f.destination.code_iata,
    origin_timezone: f.origin.timezone,
    destination_timezone: f.destination.timezone,
    flight_status: status,
    scheduled_gate_departure_at: f.scheduled_out,
    estimated_gate_departure_at: f.estimated_out,
    scheduled_gate_arrival_at: f.scheduled_in,
    estimated_gate_arrival_at: f.estimated_in,
    actual_gate_arrival_at: f.actual_in,
    boarding_cutoff_at: null,
    boarding_cutoff_provenance: 'UNKNOWN',
    boarding_cutoff_policy_version: null,
    source_revision: null,
    source_updated_at: null,
    source_timestamp_provenance: 'LOCALLY_OBSERVED',
  };
  const parsed = parseFlightUpdate(event);
  if (!parsed.ok)
    throw new Error('AeroAPI normalization violates canonical contract');
  return parsed.value;
}
