import { createHash } from 'node:crypto';
import {
  parseFlightUpdate,
  parseIngestionRejection,
  type IngestionRejectionV1,
  type ValidationIssue,
} from '@airline/contracts';
import type {
  CheckpointStore,
  EventPublisher,
  FlightProvider,
  IngestionState,
  AcquisitionContext,
  Publication,
} from '../domain/ingestion.ports.js';
import { eventIdentity } from '../domain/event-identity.js';
import type { IngestionStatus } from '../../../observability/ingestion-status.service.js';
export class FlightUpdatesStrategy {
  private state: IngestionState | undefined;
  constructor(
    private readonly provider: FlightProvider,
    private readonly checkpoint: CheckpointStore,
    private readonly publisher: EventPublisher,
    private readonly status: IngestionStatus,
    private readonly clock: () => Date = () => new Date(),
  ) {}
  async initialize(): Promise<void> {
    this.state = await this.checkpoint.load();
    this.status.initialized = true;
    this.status.runId = this.state.runId;
    this.status.nextSequence = this.state.nextSequence;
    this.status.pendingEventId = this.state.pending?.event_id ?? null;
    this.status.lastPublishedAt = this.state.lastPublishedAt;
    this.status.lastSourceEventTime = this.state.lastSourceEventTime;
  }
  async tick(signal: AbortSignal): Promise<void> {
    if (!this.state) throw new Error('Ingestion is not initialized');
    signal.throwIfAborted();
    if (!this.state.pending) {
      const context: AcquisitionContext = {
        runId: this.state.runId,
        startAt: this.state.startAt,
        sequence: this.state.nextSequence,
        observedAt: this.clock().toISOString(),
      };
      const raw = await this.acquire(context, signal);
      let event: Publication;
      try {
        const result = parseFlightUpdate(this.provider.normalize(raw, context));
        event = result.ok
          ? result.value
          : this.reject(
              raw,
              context,
              'SCHEMA_VALIDATION_FAILED',
              result.issues,
            );
      } catch {
        event = this.reject(raw, context, 'NORMALIZATION_FAILED', [
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
      const prepared = { ...this.state, pending: event };
      await this.checkpoint.save(prepared);
      this.state = prepared;
      this.status.pendingEventId = event.event_id;
    }
    const event = this.state.pending;
    if (!event) throw new Error('Missing prepared event');
    await this.publisher.publish(event);
    const now = this.clock().toISOString();
    const committed: IngestionState = {
      ...this.state,
      nextSequence: this.state.nextSequence + 1,
      pending: null,
      lastPublishedAt: now,
      lastSourceEventTime: event.event_time,
    };
    await this.checkpoint.save(committed);
    this.state = committed;
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
  private async acquire(
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
        this.provider.acquire(context, signal),
        interrupted,
      ]);
    } finally {
      signal.removeEventListener('abort', abort);
    }
  }
  private reject(
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
}
