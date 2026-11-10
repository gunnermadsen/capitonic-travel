import {
  Inject,
  Injectable,
  type OnApplicationBootstrap,
  type BeforeApplicationShutdown,
} from '@nestjs/common';
import { writeFile, rename } from 'node:fs/promises';
import { IngestionStatus } from '../observability/ingestion-status.service.js';
@Injectable()
export class HealthService
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  private timer: NodeJS.Timeout | undefined;
  private writing = false;
  private write: Promise<void> = Promise.resolve();
  constructor(
    @Inject(IngestionStatus) private readonly status: IngestionStatus,
  ) {}
  onApplicationBootstrap(): void {
    this.timer = setInterval(() => {
      if (this.writing) return;
      this.writing = true;
      this.write = this.publish()
        .catch(() => {
          this.status.healthy = false;
        })
        .finally(() => {
          this.writing = false;
        });
    }, 1000);
    this.timer.unref();
  }
  private async publish(): Promise<void> {
    await writeFile(
      '/tmp/ingester-health.pending',
      JSON.stringify({
        observedAt: new Date().toISOString(),
        live: this.status.initialized && !this.status.stopping,
        ...this.status.snapshot(),
      }),
      { mode: 0o600 },
    );
    await rename('/tmp/ingester-health.pending', '/tmp/ingester-health.json');
  }
  async beforeApplicationShutdown(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.status.stopping = true;
    await this.write;
    await this.publish();
  }
}
