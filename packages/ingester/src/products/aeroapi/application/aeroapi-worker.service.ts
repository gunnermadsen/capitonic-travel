import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type BeforeApplicationShutdown,
} from '@nestjs/common';
import { setTimeout as delay } from 'node:timers/promises';
import { AeroApiHttpError } from '../adapters/aeroapi.adapter.js';
import { AeroApiStrategy } from './aeroapi.strategy.js';
import { IngestionStatus } from '../../../observability/ingestion-status.service.js';
@Injectable()
export class AeroApiWorker
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  private readonly stop = new AbortController();
  private loop: Promise<void> | undefined;
  private readonly logger = new Logger(AeroApiWorker.name);
  constructor(
    @Inject(AeroApiStrategy) private readonly strategy: AeroApiStrategy,
    @Inject(IngestionStatus) private readonly status: IngestionStatus,
  ) {}
  async onApplicationBootstrap(): Promise<void> {
    await this.strategy.initialize();
    this.loop = this.run();
  }
  private async run(): Promise<void> {
    while (!this.stop.signal.aborted) {
      const before = this.status.requestsUsed;
      try {
        await this.strategy.tick(this.stop.signal);
        if (before !== this.status.requestsUsed)
          this.logger.log({
            event: 'aeroapi_poll_completed',
            requestsUsed: this.status.requestsUsed,
            maxRequests: this.status.maxRequests,
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
  async beforeApplicationShutdown(): Promise<void> {
    this.status.stopping = true;
    this.status.healthy = false;
    this.stop.abort();
    await this.loop;
  }
}
