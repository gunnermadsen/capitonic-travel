import { Module } from '@nestjs/common';
import { IngestionStatus } from './ingestion-status.service.js';
@Module({ providers: [IngestionStatus], exports: [IngestionStatus] })
export class ObservabilityModule {}
