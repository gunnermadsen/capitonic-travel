import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type BeforeApplicationShutdown,
} from '@nestjs/common';
import { setTimeout as delay } from 'node:timers/promises';
import {
  INGESTER_CONFIG,
  type IngesterConfiguration,
} from '../../../configuration/ingester.configuration.js';
import { IngestionStatus } from '../../../observability/ingestion-status.service.js';
import { FlightUpdatesStrategy } from './flight-updates.strategy.js';
import { FirehoseFlightUpdatesStrategy } from './firehose-flight-updates.strategy.js';
import { FirehoseAdapter } from '../adapters/firehose.adapter.js';
@Injectable()
export class FlightUpdatesWorker
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  private readonly stop = new AbortController();
  private loop: Promise<void> | undefined;
  private readonly logger = new Logger(FlightUpdatesWorker.name);
  constructor(
    @Inject(INGESTER_CONFIG) private readonly config: IngesterConfiguration,
    @Inject(FlightUpdatesStrategy)
    private readonly strategy: FlightUpdatesStrategy,
    @Inject(IngestionStatus) private readonly status: IngestionStatus,
    @Inject(FirehoseFlightUpdatesStrategy)
    private readonly firehose: FirehoseFlightUpdatesStrategy,
    @Inject(FirehoseAdapter) private readonly adapter: FirehoseAdapter,
  ) {}
  async onApplicationBootstrap(): Promise<void> {
    if (this.config.mode === 'PUBLIC') await this.firehose.initialize();
    else await this.strategy.initialize();
    this.loop = this.run();
  }
  private async run(): Promise<void> {
    while (!this.stop.signal.aborted) {
      try {
        if (this.config.mode === 'PUBLIC') {
          await this.firehose.flush();
          this.logger.log({
            event: 'firehose_connecting',
            resumePitr: this.firehose.recoveryPoint(),
          });
          for await (const raw of this.adapter.stream(
            this.firehose.recoveryPoint(),
            this.stop.signal,
          )) {
            const before = this.status.published;
            await this.firehose.accept(raw);
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
          await this.strategy.tick(
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
  async beforeApplicationShutdown(): Promise<void> {
    this.status.stopping = true;
    this.status.healthy = false;
    this.stop.abort();
    await this.loop;
  }
}
