import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import {
  parseEvent,
  parseFlightUpdate,
  type FlightUpdateV1,
  type IngestionRejectionV1,
  parseIngestionRejection,
  type ValidationIssue,
} from '@airline/contracts';
import { Logger } from '@nestjs/common';
import { setTimeout as delay } from 'node:timers/promises';
import type { IngestionStatus } from '../observability/ingestion-status.service.js';
import type { IngesterConfiguration } from '../configuration/ingester.configuration.js';
import airports from './reference/airports.json' with { type: 'json' };
import { connect } from 'node:tls';
import { StringDecoder } from 'node:string_decoder';
export type AeroApiPublication = FlightUpdateV1 | IngestionRejectionV1;
export interface AeroApiState {
  version: 1;
  requestsUsed: number;
  nextPollAt: number;
  haltReason: string | null;
  lastObservedAt: string | null;
  flights: FlightUpdateV1[];
  pending: AeroApiPublication[];
}
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
    scheduled = Date.parse(f.scheduled_out),
    arrival = Date.parse(f.estimated_in ?? f.scheduled_in ?? '');
  if (
    (scheduled < now.getTime() - 18 * 3600000 ||
      scheduled > now.getTime() + 6 * 3600000) &&
    !(
      arrival >= now.getTime() - 18 * 3600000 &&
      arrival <= now.getTime() + 6 * 3600000
    )
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
export class AeroApiHttpError extends Error {
  constructor(
    readonly status: number,
    readonly retryAfterMs: number,
  ) {
    super(`AeroAPI HTTP ${status}`);
    this.name = 'AeroApiHttpError';
  }
}
export interface AeroApiSource {
  acquire(
    schedule: AeroApiSchedule,
    now: Date,
    signal: AbortSignal,
  ): Promise<unknown[]>;
}
export interface AeroApiConfiguration {
  FLIGHTAWARE_API_KEY: string;
  airport: string;
  intervalMs: number;
  output: 'kafka' | 'validate';
  stateDirectory: string;
}
export function aeroApiConfiguration(
  env: NodeJS.ProcessEnv,
  stateDirectory: string,
  output: 'kafka' | 'validate' = 'kafka',
): AeroApiConfiguration {
  if (
    !env.FLIGHTAWARE_API_KEY ||
    !/^[A-Za-z0-9_.@+/=~-]+$/.test(env.FLIGHTAWARE_API_KEY)
  )
    throw new Error('FLIGHTAWARE_API_KEY is required');
  const airport = env.AEROAPI_AIRPORT ?? 'KJFK';
  if (!/^[A-Z]{4}$/.test(airport))
    throw new Error('AEROAPI_AIRPORT must be an ICAO airport code');
  const integer = (
    name: string,
    fallback: number,
    min: number,
    max: number,
  ) => {
    const text = env[name] ?? String(fallback),
      value = Number(text);
    if (
      !/^\d+$/.test(text) ||
      !Number.isSafeInteger(value) ||
      value < min ||
      value > max
    )
      throw new Error(`Invalid ${name}`);
    return value;
  };
  return {
    FLIGHTAWARE_API_KEY: env.FLIGHTAWARE_API_KEY,
    airport,
    intervalMs: integer('AEROAPI_POLL_INTERVAL_MS', 300000, 60000, 3600000),
    output,
    stateDirectory,
  };
}
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
export interface FirehoseState {
  version: 1;
  source: 'flightaware-firehose';
  airports: string;
  pitr: string | null;
  lastObservedAt: string | null;
  flights: Record<string, FlightState>;
  pending: { event: Publication; pitr: string } | null;
}
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
function firehoseAirport(value: unknown): { iata: string; timezone: string } {
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
    origin = firehoseAirport(f.orig),
    destination = firehoseAirport(f.dest);
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
function material(event: FlightUpdateV1): string {
  return JSON.stringify(
    Object.fromEntries(
      Object.entries(event).filter(
        ([key]) => !['event_id', 'event_time', 'ingested_at'].includes(key),
      ),
    ),
  );
}
export function eventIdentity(runId: string, key: string): string {
  const hex = createHash('sha256').update(`${runId}:${key}`).digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
export type Publication = FlightUpdateV1 | IngestionRejectionV1;
export interface AcquisitionContext {
  runId: string;
  startAt: string;
  sequence: number;
  observedAt: string;
}
export interface FlightProvider {
  acquire(context: AcquisitionContext, signal: AbortSignal): Promise<Buffer>;
  normalize(raw: Buffer, context: AcquisitionContext): unknown;
}
export interface EventPublisher {
  publish(event: Publication): Promise<void>;
}
export interface IngestionState {
  version: 1;
  runId: string;
  startAt: string;
  nextSequence: number;
  pending: Publication | null;
  lastPublishedAt: string | null;
  lastSourceEventTime: string | null;
}
export interface CheckpointStore {
  load(): Promise<IngestionState>;
  save(state: IngestionState): Promise<void>;
}
/** One product owns acquisition, normalization, recovery and lifecycle for its sources. */
export class FlightUpdatesService {
  private readonly stop = new AbortController();
  private loop: Promise<void> | undefined;
  private readonly logger = new Logger(FlightUpdatesService.name);
  readonly aeroConfig: AeroApiConfiguration;
  constructor(
    readonly config: Readonly<IngesterConfiguration>,
    readonly publisher: EventPublisher,
    readonly status: IngestionStatus,
    readonly clock: () => Date = () => new Date(),
    readonly request: typeof fetch = fetch,
    env: NodeJS.ProcessEnv = process.env,
  ) {
    this.aeroConfig =
      config.product === 'aeroapi'
        ? aeroApiConfiguration(env, config.stateDirectory, config.output)
        : {
            FLIGHTAWARE_API_KEY: '',
            airport: 'KJFK',
            intervalMs: 300000,
            output: config.output,
            stateDirectory: config.stateDirectory,
          };
  }
  async onApplicationBootstrap(): Promise<void> {
    if (this.config.product === 'aeroapi') {
      await this.aeroInitialize();
      this.loop = this.runAeroApi();
    } else {
      if (this.config.mode === 'PUBLIC') await this.firehoseInitialize();
      else await this.simInitialize();
      this.loop = this.runFlightUpdates();
    }
  }
  async beforeApplicationShutdown(): Promise<void> {
    this.status.stopping = true;
    this.status.healthy = false;
    this.stop.abort();
    await this.loop;
  }
  async aeroStoreLoad(): Promise<AeroApiState> {
    await mkdir(this.config.stateDirectory, { recursive: true, mode: 0o700 });
    let raw: string;
    try {
      raw = await readFile(
        join(this.config.stateDirectory, 'aeroapi.json'),
        'utf8',
      );
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !('code' in error) ||
        error.code !== 'ENOENT'
      )
        throw error;
      const state: AeroApiState = {
        version: 1,
        requestsUsed: 0,
        nextPollAt: 0,
        haltReason: null,
        lastObservedAt: null,
        flights: [],
        pending: [],
      };
      await this.aeroStoreSave(state);
      return state;
    }
    const value: unknown = JSON.parse(raw);
    if (
      !isRecord(value) ||
      value.version !== 1 ||
      typeof value.requestsUsed !== 'number' ||
      !Number.isSafeInteger(value.requestsUsed) ||
      value.requestsUsed < 0 ||
      typeof value.nextPollAt !== 'number' ||
      !Number.isSafeInteger(value.nextPollAt) ||
      value.nextPollAt < 0 ||
      (value.haltReason !== null && typeof value.haltReason !== 'string') ||
      (value.lastObservedAt !== null &&
        (typeof value.lastObservedAt !== 'string' ||
          !Number.isFinite(Date.parse(value.lastObservedAt)))) ||
      !Array.isArray(value.flights) ||
      !Array.isArray(value.pending)
    )
      throw new Error('Invalid AeroAPI checkpoint');
    const flights = value.flights.map((raw: unknown) => {
      const p = parseFlightUpdate(raw);
      if (
        !p.ok ||
        p.value.source !== 'flightaware-aeroapi' ||
        p.value.operating_mode !== 'PUBLIC'
      )
        throw new Error('Invalid AeroAPI checkpoint flight');
      return p.value;
    });
    const pending = value.pending.map((raw: unknown): AeroApiPublication => {
      const p = parseEvent(raw);
      if (
        !p.ok ||
        p.value.source !== 'flightaware-aeroapi' ||
        p.value.operating_mode !== 'PUBLIC' ||
        (p.value.event_type !== 'FLIGHT_UPDATED' &&
          p.value.event_type !== 'INGESTION_REJECTED')
      )
        throw new Error('Invalid AeroAPI pending event');
      return p.value;
    });
    return {
      version: 1,
      requestsUsed: value.requestsUsed,
      nextPollAt: value.nextPollAt,
      haltReason: value.haltReason,
      lastObservedAt: value.lastObservedAt,
      flights,
      pending,
    };
  }
  async aeroStoreSave(state: AeroApiState): Promise<void> {
    await this.saveCheckpoint('aeroapi.json', state);
  }
  async aeroAcquire(
    schedule: AeroApiSchedule,
    now: Date,
    signal: AbortSignal,
  ): Promise<unknown[]> {
    const url = new URL(
      `https://aeroapi.flightaware.com/aeroapi/airports/${this.aeroConfig.airport}/flights/${schedule}`,
    );
    url.searchParams.set('type', 'Airline');
    url.searchParams.set('max_pages', '1');
    url.searchParams.set(
      'start',
      new Date(now.getTime() - 2 * 3600000)
        .toISOString()
        .replace(/\.\d{3}Z$/, 'Z'),
    );
    url.searchParams.set(
      'end',
      new Date(now.getTime() + 6 * 3600000)
        .toISOString()
        .replace(/\.\d{3}Z$/, 'Z'),
    );
    const response = await this.request(url, {
      headers: {
        'x-apikey': this.aeroConfig.FLIGHTAWARE_API_KEY,
        accept: 'application/json',
      },
      redirect: 'error',
      signal: AbortSignal.any([signal, AbortSignal.timeout(15000)]),
    });
    if (!response.ok) {
      const retry = response.headers.get('retry-after');
      const wait =
        retry === null
          ? 0
          : /^\d+$/.test(retry)
            ? Number(retry) * 1000
            : Math.max(0, Date.parse(retry) - Date.now());
      await response.body?.cancel();
      throw new AeroApiHttpError(
        response.status,
        Number.isFinite(wait) ? wait : 0,
      );
    }
    const reader = response.body?.getReader();
    if (!reader) throw new Error('Empty AeroAPI response');
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        length += chunk.value.byteLength;
        if (length > 1048576) throw new Error('AeroAPI response exceeds limit');
        chunks.push(chunk.value);
      }
    } finally {
      await reader.cancel();
    }
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return parseAeroApiResponse(value, schedule);
  }
  private async runAeroApi(): Promise<void> {
    while (!this.stop.signal.aborted) {
      const before = this.status.requestsUsed;
      try {
        await this.aeroTick(this.stop.signal);
        if (before !== this.status.requestsUsed)
          this.logger.log({
            event: 'aeroapi_poll_completed',
            schedule:
              before % 2 === 0 ? 'scheduled_arrivals' : 'scheduled_departures',
            requestsUsed: this.status.requestsUsed,
            output: this.status.output,
            validated: this.status.validated,
            published: this.status.published,
            rejected: this.status.rejected,
          });
      } catch (error) {
        if (this.stop.signal.aborted) break;
        this.status.healthy = false;
        this.status.failures++;
        this.status.consecutiveFailures++;
        this.logger.warn({
          event: 'aeroapi_attempt_failed',
          errorType: error instanceof Error ? error.name : 'UnknownError',
          httpStatus: error instanceof AeroApiHttpError ? error.status : null,
          requestsUsed: this.status.requestsUsed,
          haltReason: this.status.haltReason,
        });
      }
      try {
        await delay(5000, undefined, { signal: this.stop.signal });
      } catch {
        break;
      }
    }
  }
  private aeroState: AeroApiState | undefined;
  async aeroInitialize(): Promise<void> {
    this.aeroState = await this.aeroStoreLoad();
    this.status.mode = 'PUBLIC';
    this.status.product = 'aeroapi';
    this.status.output = this.aeroConfig.output;
    this.status.publicFreshnessMs = this.aeroConfig.intervalMs * 2 + 30000;
    this.status.lastReceivedAt = this.aeroState.lastObservedAt;
    this.status.healthy =
      this.aeroState.lastObservedAt !== null &&
      this.aeroState.haltReason === null &&
      this.aeroState.pending.length === 0;
    this.status.initialized = true;
    this.aeroMetrics();
  }
  private aeroCurrent(): AeroApiState {
    if (!this.aeroState) throw new Error('AeroAPI not initialized');
    return this.aeroState;
  }
  private aeroMetrics(): void {
    const s = this.aeroCurrent();
    this.status.requestsUsed = s.requestsUsed;
    this.status.haltReason = s.haltReason;
    this.status.nextPollAt = new Date(s.nextPollAt).toISOString();
    this.status.pendingEventId = s.pending[0]?.event_id ?? null;
  }
  private async aeroCommit(next: AeroApiState): Promise<void> {
    await this.aeroStoreSave(next);
    this.aeroState = next;
    this.aeroMetrics();
  }
  private async aeroFlush(): Promise<void> {
    while (this.aeroCurrent().pending.length) {
      const s = this.aeroCurrent(),
        event = s.pending[0];
      if (!event) break;
      await this.publisher.publish(event);
      const flights =
        event.event_type === 'FLIGHT_UPDATED'
          ? [
              ...s.flights.filter(
                (f) => f.flight_instance_id !== event.flight_instance_id,
              ),
              event,
            ]
          : s.flights;
      await this.aeroCommit({ ...s, flights, pending: s.pending.slice(1) });
      if (this.aeroConfig.output === 'kafka') {
        this.status.published++;
        this.status.lastPublishedAt = this.clock().toISOString();
      } else this.status.validated++;
      if (event.event_type === 'INGESTION_REJECTED') this.status.rejected++;
      this.status.lastEventId = event.event_id;
      this.status.lastSourceEventTime = event.event_time;
    }
  }
  async aeroTick(signal: AbortSignal): Promise<void> {
    // Reload durable reservations after any prior save failure. Never reuse an attempt.
    this.aeroState = await this.aeroStoreLoad();
    this.aeroMetrics();
    await this.aeroFlush();
    let s = this.aeroCurrent();
    const now = this.clock();
    if (s.haltReason || now.getTime() < s.nextPollAt) return;
    const schedule =
      s.requestsUsed % 2 === 0 ? 'scheduled_arrivals' : 'scheduled_departures';
    // Reserve before network I/O. Persist the next poll time across crashes and failed HTTP attempts.
    await this.aeroCommit({
      ...s,
      requestsUsed: s.requestsUsed + 1,
      nextPollAt: now.getTime() + this.aeroConfig.intervalMs,
    });
    s = this.aeroCurrent();
    let rows: unknown[];
    try {
      rows = await this.aeroAcquire(schedule, now, signal);
    } catch (error) {
      if (error instanceof AeroApiHttpError) {
        await this.aeroCommit({
          ...s,
          haltReason: [401, 403].includes(error.status)
            ? `HTTP_${error.status}`
            : s.haltReason,
          nextPollAt: Math.max(
            s.nextPollAt,
            Math.min(8640000000000000, now.getTime() + error.retryAfterMs),
          ),
        });
      }
      throw error;
    }
    const observedAt = new Date(
      Math.max(
        this.clock().getTime(),
        s.lastObservedAt ? Date.parse(s.lastObservedAt) + 1 : 0,
      ),
    ).toISOString();
    this.status.lastReceivedAt = observedAt;
    const pending: AeroApiPublication[] = [];
    const ids = new Map<string, number>();
    for (const raw of rows)
      if (isRecord(raw) && typeof raw.fa_flight_id === 'string')
        ids.set(raw.fa_flight_id, (ids.get(raw.fa_flight_id) ?? 0) + 1);
    for (const [index, raw] of rows.entries()) {
      try {
        if (
          isRecord(raw) &&
          typeof raw.fa_flight_id === 'string' &&
          (ids.get(raw.fa_flight_id) ?? 0) > 1
        )
          throw new Error('Ambiguous provider flight identity');
        const previous = isRecord(raw)
          ? s.flights.find((f) => f.provider_flight_id === raw.fa_flight_id)
          : undefined;
        const event = normalizeAeroApi(
          raw,
          observedAt,
          s.requestsUsed,
          now,
          previous,
        );
        if (previous && fingerprint(previous) === fingerprint(event)) {
          this.status.duplicates++;
          continue;
        }
        pending.push(event);
      } catch {
        pending.push({
          event_id: aeroApiId(`rejection:${s.requestsUsed}:${index}`),
          schema_version: '1.0.0',
          event_type: 'INGESTION_REJECTED',
          event_time: observedAt,
          ingested_at: observedAt,
          source: 'flightaware-aeroapi',
          operating_mode: 'PUBLIC',
          data_product: 'flight_updates',
          rejected_event_id: null,
          payload_sha256: createHash('sha256')
            .update(JSON.stringify(raw) ?? 'null')
            .digest('hex'),
          reason_code: 'NORMALIZATION_FAILED',
          issues: [
            {
              path: '',
              code: 'NORMALIZATION_FAILED',
              message:
                'Missing, invalid, ambiguous, or noncurrent AeroAPI flight data.',
            },
          ],
        });
      }
    }
    await this.aeroCommit({ ...s, lastObservedAt: observedAt, pending });
    await this.aeroFlush();
    this.status.healthy = true;
    this.status.consecutiveFailures = 0;
  }

  async simStoreLoad(): Promise<IngestionState> {
    let text: string;
    try {
      text = await readFile(
        join(this.config.stateDirectory, 'flight-updates.json'),
        'utf8',
      );
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !('code' in error) ||
        error.code !== 'ENOENT'
      )
        throw error;
      const state: IngestionState = {
        version: 1,
        runId: this.config.runId ?? randomUUID(),
        startAt: this.config.startAt ?? new Date().toISOString(),
        nextSequence: 0,
        pending: null,
        lastPublishedAt: null,
        lastSourceEventTime: null,
      };
      await this.simStoreSave(state);
      return state;
    }
    const value: unknown = JSON.parse(text);
    if (
      typeof value !== 'object' ||
      value === null ||
      !('version' in value) ||
      value.version !== 1 ||
      !('runId' in value) ||
      typeof value.runId !== 'string' ||
      !('startAt' in value) ||
      typeof value.startAt !== 'string' ||
      !Number.isFinite(Date.parse(value.startAt)) ||
      !('nextSequence' in value) ||
      typeof value.nextSequence !== 'number' ||
      !Number.isSafeInteger(value.nextSequence) ||
      value.nextSequence < 0 ||
      !('pending' in value) ||
      !('lastPublishedAt' in value) ||
      !('lastSourceEventTime' in value)
    )
      throw new Error('Invalid ingestion checkpoint');
    if (
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
        value.runId,
      )
    )
      throw new Error('Invalid checkpoint run identity');
    const instant = (input: unknown): string | null => {
      if (input === null) return null;
      if (
        typeof input === 'string' &&
        Number.isFinite(Date.parse(input)) &&
        new Date(input).toISOString() === input
      )
        return input;
      throw new Error('Invalid checkpoint timestamp');
    };
    if (
      (this.config.runId && this.config.runId !== value.runId) ||
      (this.config.startAt && this.config.startAt !== value.startAt)
    )
      throw new Error(
        'Simulation configuration differs from persisted run; use a new state volume',
      );
    let pending: Publication | null = null;
    if (value.pending !== null) {
      const parsed = parseEvent(value.pending);
      if (
        !parsed.ok ||
        (parsed.value.event_type !== 'FLIGHT_UPDATED' &&
          parsed.value.event_type !== 'INGESTION_REJECTED')
      )
        throw new Error('Invalid pending event');
      pending = parsed.value;
      const key =
        pending.event_type === 'FLIGHT_UPDATED' ? 'event' : 'rejection';
      if (
        pending.event_id !==
          eventIdentity(value.runId, `${key}:${value.nextSequence}`) ||
        pending.simulation_run_id !== value.runId ||
        pending.source !== 'simulator' ||
        pending.operating_mode !== 'SYNTHETIC'
      )
        throw new Error('Pending event identity differs from checkpoint');
    }
    return {
      version: 1,
      runId: value.runId,
      startAt: value.startAt,
      nextSequence: value.nextSequence,
      pending,
      lastPublishedAt: instant(value.lastPublishedAt),
      lastSourceEventTime: instant(value.lastSourceEventTime),
    };
  }
  async simStoreSave(state: IngestionState): Promise<void> {
    await this.saveCheckpoint('flight-updates.json', state);
  }
  async firehoseStoreLoad(): Promise<FirehoseState> {
    let raw: string;
    try {
      raw = await readFile(
        join(this.config.stateDirectory, 'flight-updates-firehose.json'),
        'utf8',
      );
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !('code' in error) ||
        error.code !== 'ENOENT'
      )
        throw error;
      const state: FirehoseState = {
        version: 1,
        source: 'flightaware-firehose',
        airports: this.config.firehoseAirports,
        pitr: null,
        lastObservedAt: null,
        flights: {},
        pending: null,
      };
      await this.firehoseStoreSave(state);
      return state;
    }
    const data: unknown = JSON.parse(raw);
    if (
      !record(data) ||
      data.version !== 1 ||
      data.source !== 'flightaware-firehose' ||
      data.airports !== this.config.firehoseAirports ||
      !record(data.flights)
    )
      throw new Error('Invalid Firehose checkpoint or changed subscription');
    const state: FirehoseState = {
      version: 1,
      source: 'flightaware-firehose',
      airports: this.config.firehoseAirports,
      pitr: data.pitr === null ? null : recoveryPoint(data.pitr),
      lastObservedAt: null,
      flights: {},
      pending: null,
    };
    if (data.lastObservedAt !== null) {
      if (
        typeof data.lastObservedAt !== 'string' ||
        !Number.isFinite(Date.parse(data.lastObservedAt)) ||
        new Date(data.lastObservedAt).toISOString() !== data.lastObservedAt
      )
        throw new Error('Invalid checkpoint timestamp');
      state.lastObservedAt = data.lastObservedAt;
    }
    for (const [id, value] of Object.entries(data.flights)) {
      if (
        !record(value) ||
        !record(value.fields) ||
        !Array.isArray(value.hashes) ||
        !value.hashes.every(
          (h: unknown) => typeof h === 'string' && /^[a-f0-9]{64}$/.test(h),
        ) ||
        (value.serviceDate !== null &&
          (typeof value.serviceDate !== 'string' ||
            !/^\d{4}-\d{2}-\d{2}$/.test(value.serviceDate)))
      )
        throw new Error('Invalid checkpoint flight');
      const fields: FlightState['fields'] = {};
      for (const [key, field] of Object.entries(value.fields)) {
        if (
          field !== null &&
          typeof field !== 'string' &&
          typeof field !== 'number'
        )
          throw new Error('Invalid checkpoint field');
        fields[key] = field;
      }
      const parsed =
        value.snapshot === null ? null : parseFlightUpdate(value.snapshot);
      if (
        parsed &&
        (!parsed.ok ||
          parsed.value.source !== state.source ||
          parsed.value.operating_mode !== 'PUBLIC' ||
          parsed.value.provider_flight_id !== id)
      )
        throw new Error('Invalid checkpoint snapshot');
      state.flights[id] = {
        fields,
        serviceDate: value.serviceDate,
        pitr: recoveryPoint(value.pitr),
        hashes: value.hashes,
        snapshot: parsed?.ok ? parsed.value : null,
      };
    }
    if (data.pending !== null) {
      if (!record(data.pending)) throw new Error('Invalid pending publication');
      const parsed = parseEvent(data.pending.event);
      if (
        !parsed.ok ||
        !['FLIGHT_UPDATED', 'INGESTION_REJECTED'].includes(
          parsed.value.event_type,
        ) ||
        parsed.value.source !== state.source ||
        parsed.value.operating_mode !== 'PUBLIC'
      )
        throw new Error('Invalid pending publication');
      if (
        parsed.value.event_type === 'FLIGHT_UPDATED' ||
        parsed.value.event_type === 'INGESTION_REJECTED'
      )
        state.pending = {
          event: parsed.value,
          pitr: recoveryPoint(data.pending.pitr),
        };
    }
    return state;
  }
  async firehoseStoreSave(state: FirehoseState): Promise<void> {
    await this.saveCheckpoint('flight-updates-firehose.json', state);
  }
  private framesDecoder = new StringDecoder('utf8');
  private framesBuffer = '';
  framesPush(chunk: Buffer): string[] {
    this.framesBuffer += this.framesDecoder.write(chunk);
    const lines = this.framesBuffer.split('\n');
    this.framesBuffer = lines.pop() ?? '';
    if (
      Buffer.byteLength(this.framesBuffer) > 1048576 ||
      lines.some((line) => Buffer.byteLength(line) > 1048576)
    )
      throw new Error('Firehose frame exceeds limit');
    return lines.filter((line) => line.trim().length > 0);
  }
  framesFinish(): void {
    if ((this.framesBuffer + this.framesDecoder.end()).trim())
      throw new Error('Truncated Firehose frame');
  }
  async *firehoseStream(
    pitr: string | null,
    signal: AbortSignal,
  ): AsyncGenerator<string> {
    this.framesDecoder = new StringDecoder('utf8');
    this.framesBuffer = '';
    signal.throwIfAborted();
    const socket = connect({
      host: 'firehose.flightaware.com',
      port: 1501,
      servername: 'firehose.flightaware.com',
      minVersion: 'TLSv1.2',
      rejectUnauthorized: true,
    });
    let closeDeadline: ReturnType<typeof setTimeout> | undefined;
    const stop = () => {
      socket.end();
      closeDeadline = setTimeout(() => socket.destroy(), 1000);
    };
    signal.addEventListener('abort', stop, { once: true });
    const deadline = setTimeout(
      () => socket.destroy(new Error('Firehose connection timeout')),
      10000,
    );
    socket.setTimeout(90000, () =>
      socket.destroy(new Error('Firehose idle timeout')),
    );
    socket.once('secureConnect', () => {
      clearTimeout(deadline);
      const start = pitr === null ? 'live' : `pitr ${pitr}`;
      socket.write(
        `${start} username ${this.config.FIREHOSE_USERNAME} password ${this.config.FIREHOSE_API_KEY} events "flifo departure arrival cancellation" airport_filter "${this.config.firehoseAirports}" optype_filter airline keepalive 15\n`,
      );
    });
    try {
      // Async iteration pauses socket reads while the caller persists/publishes each item.
      for await (const chunk of socket) {
        if (!Buffer.isBuffer(chunk))
          throw new Error('Unexpected socket encoding');
        for (const line of this.framesPush(chunk)) {
          signal.throwIfAborted();
          yield line;
        }
      }
      if (!signal.aborted) {
        this.framesFinish();
        throw new Error('Firehose disconnected');
      }
    } finally {
      clearTimeout(deadline);
      if (closeDeadline) clearTimeout(closeDeadline);
      signal.removeEventListener('abort', stop);
      socket.destroy();
    }
  }
  async simSourceAcquire(
    context: AcquisitionContext,
    signal: AbortSignal,
  ): Promise<Buffer> {
    signal.throwIfAborted();
    const step = context.sequence % 6;
    return Buffer.from(
      JSON.stringify({
        direction: step % 2 === 0 ? 'inbound' : 'outbound',
        delayMinutes: step === 2 ? 55 : step === 3 ? 60 : 0,
        status:
          step === 4
            ? 'CANCELLED'
            : step === 2 || step === 3
              ? 'DELAYED'
              : 'SCHEDULED',
      }),
    );
  }
  simSourceNormalize(raw: Buffer, context: AcquisitionContext): FlightUpdateV1 {
    const value: unknown = JSON.parse(raw.toString('utf8'));
    if (
      typeof value !== 'object' ||
      value === null ||
      !('direction' in value) ||
      !('delayMinutes' in value) ||
      !('status' in value) ||
      !['inbound', 'outbound'].includes(String(value.direction)) ||
      typeof value.delayMinutes !== 'number' ||
      !Number.isInteger(value.delayMinutes) ||
      value.delayMinutes < 0 ||
      value.delayMinutes > 1440 ||
      (value.status !== 'SCHEDULED' &&
        value.status !== 'DELAYED' &&
        value.status !== 'CANCELLED')
    )
      throw new Error('Invalid simulator snapshot');
    const inbound = value.direction === 'inbound',
      cycle = Math.floor(context.sequence / 6),
      base = Date.parse(context.startAt) + cycle * 86400000;
    const departure = base + (inbound ? 0 : 7200000),
      arrival = base + (inbound ? 3600000 : 10800000),
      sourceTime = new Date(
        Date.parse(context.startAt) + context.sequence * 1000,
      ).toISOString();
    const originTimezone = inbound ? 'America/Los_Angeles' : 'America/New_York';
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: originTimezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(new Date(departure));
    const localDate = ['year', 'month', 'day']
      .map((kind) => parts.find((part) => part.type === kind)?.value)
      .join('-');
    return {
      event_id: eventIdentity(context.runId, `event:${context.sequence}`),
      schema_version: '1.0.0',
      event_type: 'FLIGHT_UPDATED',
      event_time: sourceTime,
      ingested_at: context.observedAt,
      source: 'simulator',
      operating_mode: 'SYNTHETIC',
      simulation_run_id: context.runId,
      flight_instance_id: eventIdentity(
        context.runId,
        `flight:${cycle}:${value.direction}`,
      ),
      provider_flight_id: null,
      operating_carrier: 'AA',
      flight_number: inbound ? '123' : '456',
      service_date: localDate,
      origin_timezone: originTimezone,
      destination_timezone: 'America/New_York',
      origin: inbound ? 'LAX' : 'JFK',
      destination: inbound ? 'JFK' : 'BOS',
      flight_status: value.status,
      scheduled_gate_arrival_at: new Date(arrival).toISOString(),
      estimated_gate_arrival_at: value.delayMinutes
        ? new Date(arrival + value.delayMinutes * 60000).toISOString()
        : null,
      actual_gate_arrival_at: null,
      scheduled_gate_departure_at: new Date(departure).toISOString(),
      estimated_gate_departure_at: value.delayMinutes
        ? new Date(departure + value.delayMinutes * 60000).toISOString()
        : null,
      boarding_cutoff_at: inbound
        ? null
        : new Date(
            departure + value.delayMinutes * 60000 - 1800000,
          ).toISOString(),
      source_revision: Math.floor((context.sequence % 6) / 2),
      source_updated_at: sourceTime,
      source_timestamp_provenance: 'SOURCE_ISSUED',
      boarding_cutoff_provenance: inbound ? 'UNKNOWN' : 'INFERRED_POLICY',
      boarding_cutoff_policy_version: inbound ? null : 'simulator-cutoff-v1',
    };
  }
  private firehoseState: FirehoseState | undefined;
  async firehoseInitialize(): Promise<void> {
    this.firehoseState = await this.firehoseStoreLoad();
    this.status.initialized = true;
    this.status.mode = 'PUBLIC';
    this.status.runId = null;
    await this.firehoseFlush();
  }
  firehoseRecoveryPoint(): string | null {
    return this.firehoseCurrent().pitr;
  }
  private firehoseCurrent(): FirehoseState {
    if (!this.firehoseState)
      throw new Error('Firehose strategy not initialized');
    return this.firehoseState;
  }
  async firehoseFlush(): Promise<void> {
    const state = this.firehoseCurrent(),
      pending = state.pending;
    if (!pending) return;
    this.status.pendingEventId = pending.event.event_id;
    await this.publisher.publish(pending.event);
    const committed = {
      ...state,
      pending: null,
      pitr:
        state.pitr === null || comparePitr(pending.pitr, state.pitr) > 0
          ? pending.pitr
          : state.pitr,
    };
    await this.firehoseStoreSave(committed);
    this.firehoseState = committed;
    this.status.pendingEventId = null;
    this.status.lastEventId = pending.event.event_id;
    this.status.published++;
    if (pending.event.event_type === 'INGESTION_REJECTED')
      this.status.rejected++;
    this.status.lastPublishedAt = this.clock().toISOString();
    this.status.lastSourceEventTime = pending.event.event_time;
    this.status.healthy = true;
    this.status.consecutiveFailures = 0;
  }
  async firehoseAccept(raw: string): Promise<void> {
    await this.firehoseFlush();
    const input: unknown = JSON.parse(raw);
    if (record(input) && input.type === 'error') {
      const error = new Error('Firehose rejected the requested stream');
      error.name =
        typeof input.error_msg === 'string' &&
        input.error_msg.includes('Maximum simultaneous connection')
          ? 'FirehoseConnectionLimitError'
          : 'FirehoseSubscriptionError';
      throw error;
    }
    if (
      !record(input) ||
      input.type === 'error' ||
      input.error ||
      typeof input.type !== 'string'
    )
      throw new Error('Firehose protocol or subscription error');
    const pitr = recoveryPoint(input.pitr),
      state = this.firehoseCurrent();
    this.status.lastReceivedAt = this.clock().toISOString();
    if (input.type === 'keepalive') {
      const next = {
        ...state,
        pitr:
          state.pitr === null || comparePitr(pitr, state.pitr) > 0
            ? pitr
            : state.pitr,
      };
      await this.firehoseStoreSave(next);
      this.firehoseState = next;
      this.status.healthy = true;
      return;
    }
    if (!['flifo', 'departure', 'arrival', 'cancellation'].includes(input.type))
      throw new Error('Unexpected Firehose event');
    const hash = createHash('sha256').update(raw).digest('hex');
    const id =
      typeof input.id === 'string' && /^[A-Za-z0-9._-]{1,256}$/.test(input.id)
        ? input.id
        : null;
    if (!id)
      throw new Error('Firehose message lacks recoverable flight identity');
    const previous = state.flights[id];
    if (
      previous &&
      (comparePitr(pitr, previous.pitr) < 0 ||
        (comparePitr(pitr, previous.pitr) === 0 &&
          previous.hashes.includes(hash)))
    ) {
      this.status.duplicates++;
      return;
    }
    if (id && !previous && Object.keys(state.flights).length >= 5000)
      throw new Error('Firehose state capacity reached; narrow subscription');
    if (
      previous &&
      comparePitr(pitr, previous.pitr) === 0 &&
      previous.hashes.length >= 10000
    )
      throw new Error('Firehose per-cursor capacity reached');
    const observedAt = new Date(
      Math.max(
        this.clock().getTime(),
        Date.parse(state.lastObservedAt ?? '1970-01-01T00:00:00.000Z') + 1,
      ),
    ).toISOString();
    const next: FirehoseState = {
      ...state,
      flights: { ...state.flights },
      lastObservedAt: observedAt,
    };
    // Even rejected updates retain replay identity without applying invalid fields.
    next.flights[id] = {
      fields: { ...previous?.fields },
      serviceDate: previous?.serviceDate ?? null,
      pitr,
      hashes:
        previous && comparePitr(previous.pitr, pitr) === 0
          ? [...previous.hashes, hash]
          : [hash],
      snapshot: previous?.snapshot ?? null,
    };
    let event: Publication | null = null;
    try {
      if (!id) throw new Error('Missing provider flight identity');
      const flight = mergeFlight(input, previous, pitr, hash);
      next.flights[id] = flight;
      const normalized = normalizeFirehose(id, flight, hash, observedAt);
      flight.serviceDate = normalized.service_date;
      flight.snapshot = normalized;
      if (
        previous?.snapshot &&
        material(previous.snapshot) === material(normalized)
      ) {
        flight.snapshot = previous.snapshot;
      } else event = normalized;
    } catch {
      event = {
        event_id: eventIdentity(
          'flightaware-firehose',
          `rejection:${pitr}:${hash}`,
        ),
        schema_version: '1.0.0',
        event_type: 'INGESTION_REJECTED',
        event_time: observedAt,
        ingested_at: observedAt,
        source: 'flightaware-firehose',
        operating_mode: 'PUBLIC',
        data_product: 'flight_updates',
        rejected_event_id: null,
        payload_sha256: hash,
        reason_code: 'NORMALIZATION_FAILED',
        issues: [
          {
            path: '',
            code: 'NORMALIZATION_FAILED',
            message:
              'Required flight identity, airport metadata, scheduled service date, or valid fields unavailable.',
          },
        ],
      };
      if (!parseIngestionRejection(event).ok)
        throw new Error('Invalid Firehose rejection');
    }
    if (event === null) {
      next.pitr =
        state.pitr === null || comparePitr(pitr, state.pitr) > 0
          ? pitr
          : state.pitr;
      await this.firehoseStoreSave(next);
      this.firehoseState = next;
      this.status.duplicates++;
      this.status.healthy = true;
      return;
    }
    next.pending = { event, pitr };
    await this.firehoseStoreSave(next);
    this.firehoseState = next;
    await this.firehoseFlush();
  }
  private async runFlightUpdates(): Promise<void> {
    while (!this.stop.signal.aborted) {
      try {
        if (this.config.mode === 'PUBLIC') {
          await this.firehoseFlush();
          this.logger.log({
            event: 'firehose_connecting',
            resumePitr: this.firehoseRecoveryPoint(),
          });
          for await (const raw of this.firehoseStream(
            this.firehoseRecoveryPoint(),
            this.stop.signal,
          )) {
            const before = this.status.published;
            await this.firehoseAccept(raw);
            if (this.status.published !== before)
              this.logger.log({
                event: 'ingestion_committed',
                source: 'flightaware-firehose',
                eventId: this.status.lastEventId,
                published: this.status.published,
                rejected: this.status.rejected,
              });
          }
        } else
          await this.simTick(
            AbortSignal.any([
              this.stop.signal,
              AbortSignal.timeout(this.config.acquisitionTimeoutMs),
            ]),
          );
        this.logger.log({
          event: 'ingestion_committed',
          eventId: this.status.lastEventId,
          simulationRunId: this.status.runId,
          nextSequence: this.status.nextSequence,
        });
      } catch (error) {
        if (this.stop.signal.aborted) break;
        this.status.healthy = false;
        this.status.failures++;
        this.status.consecutiveFailures++;
        this.logger.warn({
          event: 'ingestion_retry',
          eventId: this.status.pendingEventId,
          simulationRunId: this.status.runId,
          attempt: this.status.consecutiveFailures,
          errorType: error instanceof Error ? error.name : 'UnknownError',
        });
      }
      try {
        await delay(
          Math.min(
            30000,
            this.config.intervalMs *
              2 ** Math.min(this.status.consecutiveFailures, 5),
          ),
          undefined,
          { signal: this.stop.signal },
        );
      } catch {
        break;
      }
    }
  }
  private simState: IngestionState | undefined;
  async simInitialize(): Promise<void> {
    this.simState = await this.simStoreLoad();
    this.status.initialized = true;
    this.status.runId = this.simState.runId;
    this.status.nextSequence = this.simState.nextSequence;
    this.status.pendingEventId = this.simState.pending?.event_id ?? null;
    this.status.lastPublishedAt = this.simState.lastPublishedAt;
    this.status.lastSourceEventTime = this.simState.lastSourceEventTime;
  }
  async simTick(signal: AbortSignal): Promise<void> {
    if (!this.simState) throw new Error('Ingestion is not initialized');
    signal.throwIfAborted();
    if (!this.simState.pending) {
      const context: AcquisitionContext = {
        runId: this.simState.runId,
        startAt: this.simState.startAt,
        sequence: this.simState.nextSequence,
        observedAt: this.clock().toISOString(),
      };
      const raw = await this.simAcquire(context, signal);
      let event: Publication;
      try {
        const result = parseFlightUpdate(this.simSourceNormalize(raw, context));
        event = result.ok
          ? result.value
          : this.simReject(
              raw,
              context,
              'SCHEMA_VALIDATION_FAILED',
              result.issues,
            );
      } catch {
        event = this.simReject(raw, context, 'NORMALIZATION_FAILED', [
          {
            path: '',
            code: 'NORMALIZATION_FAILED',
            message: 'Provider snapshot could not be normalized.',
          },
        ]);
      }
      if (
        event.event_type === 'FLIGHT_UPDATED' &&
        (event.event_id !==
          eventIdentity(context.runId, `event:${context.sequence}`) ||
          event.simulation_run_id !== context.runId ||
          event.source !== 'simulator')
      )
        throw new Error(
          'Adapter violated authoritative source or retry identity',
        );
      signal.throwIfAborted();
      const prepared = { ...this.simState, pending: event };
      await this.simStoreSave(prepared);
      this.simState = prepared;
      this.status.pendingEventId = event.event_id;
    }
    const event = this.simState.pending;
    if (!event) throw new Error('Missing prepared event');
    await this.publisher.publish(event);
    const now = this.clock().toISOString();
    const committed: IngestionState = {
      ...this.simState,
      nextSequence: this.simState.nextSequence + 1,
      pending: null,
      lastPublishedAt: now,
      lastSourceEventTime: event.event_time,
    };
    await this.simStoreSave(committed);
    this.simState = committed;
    this.status.pendingEventId = null;
    this.status.lastEventId = event.event_id;
    this.status.nextSequence = committed.nextSequence;
    this.status.published++;
    if (event.event_type === 'INGESTION_REJECTED') this.status.rejected++;
    this.status.lastPublishedAt = now;
    this.status.lastSourceEventTime = event.event_time;
    this.status.healthy = true;
    this.status.consecutiveFailures = 0;
  }
  private async simAcquire(
    context: AcquisitionContext,
    signal: AbortSignal,
  ): Promise<Buffer> {
    let abort: () => void = () => {};
    const interrupted = new Promise<never>((_, reject) => {
      abort = () => reject(new Error('Provider acquisition interrupted'));
      signal.addEventListener('abort', abort, { once: true });
    });
    try {
      signal.throwIfAborted();
      return await Promise.race([
        this.simSourceAcquire(context, signal),
        interrupted,
      ]);
    } finally {
      signal.removeEventListener('abort', abort);
    }
  }
  private simReject(
    raw: Buffer,
    context: AcquisitionContext,
    reason: IngestionRejectionV1['reason_code'],
    issues: ValidationIssue[],
  ): IngestionRejectionV1 {
    const event: IngestionRejectionV1 = {
      event_id: eventIdentity(context.runId, `rejection:${context.sequence}`),
      schema_version: '1.0.0',
      event_type: 'INGESTION_REJECTED',
      event_time: context.observedAt,
      ingested_at: context.observedAt,
      source: 'simulator',
      operating_mode: 'SYNTHETIC',
      simulation_run_id: context.runId,
      data_product: 'flight_updates',
      rejected_event_id: null,
      payload_sha256: createHash('sha256').update(raw).digest('hex'),
      reason_code: reason,
      issues,
    };
    if (!parseIngestionRejection(event).ok)
      throw new Error('Invalid rejection event');
    return event;
  }

  private async saveCheckpoint(
    name: string,
    state: AeroApiState | FirehoseState | IngestionState,
  ): Promise<void> {
    const directory = this.config.stateDirectory;
    const temporary = join(directory, `.${name}-${randomUUID()}.tmp`);
    try {
      const file = await open(temporary, 'wx', 0o600);
      try {
        await file.writeFile(JSON.stringify(state));
        await file.sync();
      } finally {
        await file.close();
      }
      await rename(temporary, join(directory, name));
      const dir = await open(directory, 'r');
      try {
        await dir.sync();
      } finally {
        await dir.close();
      }
    } finally {
      await unlink(temporary).catch(() => {});
    }
  }
}
