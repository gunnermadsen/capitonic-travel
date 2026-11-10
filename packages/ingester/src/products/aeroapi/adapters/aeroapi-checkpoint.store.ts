import { open, readFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import {
  parseEvent,
  parseFlightUpdate,
  type FlightUpdateV1,
  type IngestionRejectionV1,
} from '@airline/contracts';
import { isRecord } from '../domain/aeroapi.contract.js';
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
export class AeroApiCheckpointStore {
  constructor(private readonly directory: string) {}
  async load(): Promise<AeroApiState> {
    let raw: string;
    try {
      raw = await readFile(join(this.directory, 'aeroapi.json'), 'utf8');
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
      await this.save(state);
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
  async save(state: AeroApiState): Promise<void> {
    const temporary = join(this.directory, `.aeroapi-${randomUUID()}.tmp`);
    try {
      const file = await open(temporary, 'wx', 0o600);
      try {
        await file.writeFile(JSON.stringify(state));
        await file.sync();
      } finally {
        await file.close();
      }
      await rename(temporary, join(this.directory, 'aeroapi.json'));
      const directory = await open(this.directory, 'r');
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    } finally {
      await unlink(temporary).catch(() => {});
    }
  }
}
