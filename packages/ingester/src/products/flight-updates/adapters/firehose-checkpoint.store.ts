import { open, readFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { parseEvent, parseFlightUpdate } from '@airline/contracts';
import type { Publication } from '../domain/ingestion.ports.js';
import {
  record,
  recoveryPoint,
  type FlightState,
} from './firehose-normalization.js';
export interface FirehoseState {
  version: 1;
  source: 'flightaware-firehose';
  airports: string;
  pitr: string | null;
  lastObservedAt: string | null;
  flights: Record<string, FlightState>;
  pending: { event: Publication; pitr: string } | null;
}
export class FirehoseCheckpointStore {
  constructor(
    private readonly directory: string,
    private readonly airports: string,
  ) {}
  async load(): Promise<FirehoseState> {
    let raw: string;
    try {
      raw = await readFile(
        join(this.directory, 'flight-updates-firehose.json'),
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
        airports: this.airports,
        pitr: null,
        lastObservedAt: null,
        flights: {},
        pending: null,
      };
      await this.save(state);
      return state;
    }
    const data: unknown = JSON.parse(raw);
    if (
      !record(data) ||
      data.version !== 1 ||
      data.source !== 'flightaware-firehose' ||
      data.airports !== this.airports ||
      !record(data.flights)
    )
      throw new Error('Invalid Firehose checkpoint or changed subscription');
    const state: FirehoseState = {
      version: 1,
      source: 'flightaware-firehose',
      airports: this.airports,
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
  async save(state: FirehoseState): Promise<void> {
    const temporary = join(this.directory, `.firehose-${randomUUID()}.tmp`);
    try {
      const file = await open(temporary, 'wx', 0o600);
      try {
        await file.writeFile(JSON.stringify(state));
        await file.sync();
      } finally {
        await file.close();
      }
      await rename(
        temporary,
        join(this.directory, 'flight-updates-firehose.json'),
      );
      const dir = await open(this.directory, 'r');
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
