import { createHash } from 'node:crypto';
import {
  parseIngestionRejection,
  type FlightUpdateV1,
} from '@airline/contracts';
import type { EventPublisher, Publication } from '../domain/ingestion.ports.js';
import { eventIdentity } from '../domain/event-identity.js';
import {
  comparePitr,
  mergeFlight,
  normalizeFirehose,
  record,
  recoveryPoint,
} from '../adapters/firehose-normalization.js';
import type {
  FirehoseCheckpointStore,
  FirehoseState,
} from '../adapters/firehose-checkpoint.store.js';
import type { IngestionStatus } from '../../../observability/ingestion-status.service.js';
function material(event: FlightUpdateV1): string {
  return JSON.stringify(
    Object.fromEntries(
      Object.entries(event).filter(
        ([key]) => !['event_id', 'event_time', 'ingested_at'].includes(key),
      ),
    ),
  );
}
export class FirehoseFlightUpdatesStrategy {
  private state: FirehoseState | undefined;
  constructor(
    private readonly store: Pick<FirehoseCheckpointStore, 'load' | 'save'>,
    private readonly publisher: EventPublisher,
    private readonly status: IngestionStatus,
    private readonly clock: () => Date = () => new Date(),
  ) {}
  async initialize(): Promise<void> {
    this.state = await this.store.load();
    this.status.initialized = true;
    this.status.mode = 'PUBLIC';
    this.status.runId = null;
    await this.flush();
  }
  recoveryPoint(): string | null {
    return this.current().pitr;
  }
  private current(): FirehoseState {
    if (!this.state) throw new Error('Firehose strategy not initialized');
    return this.state;
  }
  async flush(): Promise<void> {
    const state = this.current(),
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
    await this.store.save(committed);
    this.state = committed;
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
  async accept(raw: string): Promise<void> {
    await this.flush();
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
      state = this.current();
    this.status.lastReceivedAt = this.clock().toISOString();
    if (input.type === 'keepalive') {
      const next = {
        ...state,
        pitr:
          state.pitr === null || comparePitr(pitr, state.pitr) > 0
            ? pitr
            : state.pitr,
      };
      await this.store.save(next);
      this.state = next;
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
      await this.store.save(next);
      this.state = next;
      this.status.duplicates++;
      this.status.healthy = true;
      return;
    }
    next.pending = { event, pitr };
    await this.store.save(next);
    this.state = next;
    await this.flush();
  }
}
