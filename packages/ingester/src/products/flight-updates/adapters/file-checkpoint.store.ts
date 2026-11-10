import { open, readFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { parseEvent } from '@airline/contracts';
import type { IngesterConfiguration } from '../../../configuration/ingester.configuration.js';
import type {
  CheckpointStore,
  IngestionState,
  Publication,
} from '../domain/ingestion.ports.js';
import { eventIdentity } from '../domain/event-identity.js';
export class FileCheckpointStore implements CheckpointStore {
  private readonly path: string;
  constructor(private readonly config: Readonly<IngesterConfiguration>) {
    this.path = join(config.stateDirectory, 'flight-updates.json');
  }
  async load(): Promise<IngestionState> {
    let text: string;
    try {
      text = await readFile(this.path, 'utf8');
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
      await this.save(state);
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
  async save(state: IngestionState): Promise<void> {
    // Compose holds an OS flock for the process lifetime. Pending payload is durable before send.
    const temporary = join(
      this.config.stateDirectory,
      `.checkpoint-${randomUUID()}.tmp`,
    );
    try {
      const file = await open(temporary, 'wx', 0o600);
      try {
        await file.writeFile(JSON.stringify(state));
        await file.sync();
      } finally {
        await file.close();
      }
      await rename(temporary, this.path);
      const directory = await open(this.config.stateDirectory, 'r');
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
