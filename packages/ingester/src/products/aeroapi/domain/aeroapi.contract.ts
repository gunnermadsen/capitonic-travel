/** Provider boundary only. Canonical event schemas remain in @airline/contracts. */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
export type AeroApiSchedule = 'scheduled_arrivals' | 'scheduled_departures';
export function parseAeroApiResponse(
  value: unknown,
  schedule: AeroApiSchedule,
): unknown[] {
  if (
    !isRecord(value) ||
    !Array.isArray(value[schedule]) ||
    value[schedule].length > 15 ||
    value.num_pages !== 1
  )
    throw new Error('Invalid or oversized AeroAPI response');
  // Deliberately never follow links.next: every request is limited to one result page.
  return value[schedule];
}
export interface AeroApiAirport {
  code_iata: string;
  timezone: string;
}
export interface AeroApiFlight {
  fa_flight_id: string;
  operator_icao: string;
  flight_number: string;
  origin: AeroApiAirport;
  destination: AeroApiAirport;
  scheduled_out: string;
  scheduled_in: string | null;
  estimated_out: string | null;
  estimated_in: string | null;
  actual_out: string | null;
  actual_off: string | null;
  actual_on: string | null;
  actual_in: string | null;
  cancelled: boolean;
  diverted: boolean;
  status: string;
}
function airport(value: unknown): AeroApiAirport {
  if (
    !isRecord(value) ||
    typeof value.code_iata !== 'string' ||
    !/^[A-Z]{3}$/.test(value.code_iata) ||
    typeof value.timezone !== 'string'
  )
    throw new Error('Missing airport metadata');
  new Intl.DateTimeFormat('en-US', { timeZone: value.timezone });
  return { code_iata: value.code_iata, timezone: value.timezone };
}
function time(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (
    typeof value !== 'string' ||
    !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(
      value,
    ) ||
    !Number.isFinite(Date.parse(value))
  )
    throw new Error('Invalid AeroAPI timestamp');
  const date = value.slice(0, 10);
  if (new Date(`${date}T00:00:00Z`).toISOString().slice(0, 10) !== date)
    throw new Error('Invalid AeroAPI calendar date');
  return new Date(value).toISOString();
}
export function parseAeroApiFlight(value: unknown): AeroApiFlight {
  if (
    !isRecord(value) ||
    typeof value.fa_flight_id !== 'string' ||
    !/^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$/.test(value.fa_flight_id) ||
    typeof value.operator_icao !== 'string' ||
    !/^[A-Z]{3}$/.test(value.operator_icao) ||
    typeof value.flight_number !== 'string' ||
    !/^[0-9]{1,4}[A-Z]?$/.test(value.flight_number) ||
    typeof value.cancelled !== 'boolean' ||
    typeof value.diverted !== 'boolean' ||
    typeof value.status !== 'string'
  )
    throw new Error('Invalid AeroAPI flight');
  const scheduled_out = time(value.scheduled_out);
  if (!scheduled_out) throw new Error('Missing scheduled gate departure');
  return {
    fa_flight_id: value.fa_flight_id,
    operator_icao: value.operator_icao,
    flight_number: value.flight_number,
    origin: airport(value.origin),
    destination: airport(value.destination),
    scheduled_out,
    scheduled_in: time(value.scheduled_in),
    estimated_out: time(value.estimated_out),
    estimated_in: time(value.estimated_in),
    actual_out: time(value.actual_out),
    actual_off: time(value.actual_off),
    actual_on: time(value.actual_on),
    actual_in: time(value.actual_in),
    cancelled: value.cancelled,
    diverted: value.diverted,
    status: value.status,
  };
}
