import { createHash } from 'node:crypto';
import type { AeroApiConfiguration } from '../aeroapi.configuration.js';
import {
  AeroApiHttpError,
  type AeroApiSource,
} from '../adapters/aeroapi.adapter.js';
import {
  AeroApiCheckpointStore,
  type AeroApiState,
  type AeroApiPublication,
} from '../adapters/aeroapi-checkpoint.store.js';
import {
  aeroApiId,
  fingerprint,
  normalizeAeroApi,
} from '../adapters/aeroapi-normalization.js';
import { isRecord } from '../domain/aeroapi.contract.js';
import { IngestionStatus } from '../../../observability/ingestion-status.service.js';
export class AeroApiStrategy {
  private state: AeroApiState | undefined;
  constructor(
    private readonly config: AeroApiConfiguration,
    private readonly source: AeroApiSource,
    private readonly store: AeroApiCheckpointStore,
    private readonly publisher: {
      publish(event: AeroApiPublication): Promise<void>;
    },
    private readonly status: IngestionStatus,
    private readonly clock: () => Date = () => new Date(),
  ) {}
  async initialize(): Promise<void> {
    this.state = await this.store.load();
    this.status.mode = 'PUBLIC';
    this.status.product = 'aeroapi';
    this.status.publicFreshnessMs = this.config.intervalMs * 2 + 30000;
    this.status.lastReceivedAt = this.state.lastObservedAt;
    this.status.healthy =
      this.state.lastObservedAt !== null &&
      this.state.haltReason === null &&
      this.state.pending.length === 0;
    this.status.initialized = true;
    this.metrics();
  }
  private current(): AeroApiState {
    if (!this.state) throw new Error('AeroAPI not initialized');
    return this.state;
  }
  private metrics(): void {
    const s = this.current();
    this.status.requestsUsed = s.requestsUsed;
    this.status.maxRequests = this.config.maxRequests;
    this.status.budgetExhausted = s.requestsUsed >= this.config.maxRequests;
    this.status.haltReason = s.haltReason;
    this.status.nextPollAt = new Date(s.nextPollAt).toISOString();
    this.status.pendingEventId = s.pending[0]?.event_id ?? null;
  }
  private async commit(next: AeroApiState): Promise<void> {
    await this.store.save(next);
    this.state = next;
    this.metrics();
  }
  private async flush(): Promise<void> {
    while (this.current().pending.length) {
      const s = this.current(),
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
      await this.commit({ ...s, flights, pending: s.pending.slice(1) });
      this.status.published++;
      if (event.event_type === 'INGESTION_REJECTED') this.status.rejected++;
      this.status.lastEventId = event.event_id;
      this.status.lastPublishedAt = this.clock().toISOString();
      this.status.lastSourceEventTime = event.event_time;
    }
  }
  async tick(signal: AbortSignal): Promise<void> {
    // Reload durable reservations after any prior save failure. Never reuse an attempt.
    this.state = await this.store.load();
    this.metrics();
    await this.flush();
    let s = this.current();
    const now = this.clock();
    if (
      s.haltReason ||
      s.requestsUsed >= this.config.maxRequests ||
      now.getTime() < s.nextPollAt
    )
      return;
    const ident =
      this.config.idents[s.requestsUsed % this.config.idents.length];
    if (!ident) throw new Error('No AeroAPI flight configured');
    // Reserve before network I/O. Crashes and failed HTTP attempts still consume quota.
    await this.commit({
      ...s,
      requestsUsed: s.requestsUsed + 1,
      nextPollAt: now.getTime() + this.config.intervalMs,
    });
    s = this.current();
    let rows: unknown[];
    try {
      rows = await this.source.acquire(ident, now, signal);
    } catch (error) {
      if (error instanceof AeroApiHttpError) {
        await this.commit({
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
    await this.commit({ ...s, lastObservedAt: observedAt, pending });
    await this.flush();
    this.status.healthy = true;
    this.status.consecutiveFailures = 0;
  }
}
