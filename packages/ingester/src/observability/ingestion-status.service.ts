import { Injectable } from '@nestjs/common';
@Injectable()
export class IngestionStatus {
  product = 'flight-updates';
  publicFreshnessMs = 90000;
  requestsUsed = 0;
  maxRequests = 0;
  budgetExhausted = false;
  haltReason: string | null = null;
  nextPollAt: string | null = null;
  mode: 'PUBLIC' | 'SYNTHETIC' = 'SYNTHETIC';
  lastReceivedAt: string | null = null;
  duplicates = 0;
  initialized = false;
  stopping = false;
  healthy = false;
  runId: string | null = null;
  pendingEventId: string | null = null;
  lastEventId: string | null = null;
  nextSequence = 0;
  lastPublishedAt: string | null = null;
  lastSourceEventTime: string | null = null;
  published = 0;
  rejected = 0;
  failures = 0;
  consecutiveFailures = 0;
  snapshot() {
    const publicationAgeSeconds = this.lastPublishedAt
      ? Math.max(0, (Date.now() - Date.parse(this.lastPublishedAt)) / 1000)
      : null;
    return {
      ready:
        this.initialized &&
        this.healthy &&
        !this.stopping &&
        (this.mode === 'PUBLIC'
          ? this.lastReceivedAt !== null &&
            Date.now() - Date.parse(this.lastReceivedAt) <
              this.publicFreshnessMs
          : publicationAgeSeconds !== null && publicationAgeSeconds < 120),
      mode: this.mode,
      product: this.product,
      requestsUsed: this.requestsUsed,
      maxRequests: this.maxRequests,
      budgetExhausted: this.budgetExhausted,
      haltReason: this.haltReason,
      nextPollAt: this.nextPollAt,
      lastReceivedAt: this.lastReceivedAt,
      duplicates: this.duplicates,
      stopping: this.stopping,
      simulationRunId: this.runId,
      pendingEventId: this.pendingEventId,
      lastEventId: this.lastEventId,
      nextSequence: this.nextSequence,
      published: this.published,
      rejected: this.rejected,
      failures: this.failures,
      consecutiveFailures: this.consecutiveFailures,
      lastPublishedAt: this.lastPublishedAt,
      lastSourceEventTime: this.lastSourceEventTime,
      publicationAgeSeconds,
      sourceAgeSeconds: this.lastSourceEventTime
        ? Math.max(
            0,
            (Date.now() - Date.parse(this.lastSourceEventTime)) / 1000,
          )
        : null,
    };
  }
}
