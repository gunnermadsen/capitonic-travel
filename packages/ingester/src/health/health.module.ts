import { Module } from '@nestjs/common';
import { ObservabilityModule } from '../observability/observability.module.js';
import { HealthService } from './health.service.js';
@Module({ imports: [ObservabilityModule], providers: [HealthService] })
export class HealthModule {}
