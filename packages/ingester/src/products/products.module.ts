import { Module } from '@nestjs/common';
import { ConfigurationModule } from '../configuration/configuration.module.js';
import {
  INGESTER_CONFIG,
  type IngesterConfiguration,
} from '../configuration/ingester.configuration.js';
import { KafkaProducerModule } from '../kafka/kafka-producer.module.js';
import { KafkaProducerService } from '../kafka/kafka-producer.service.js';
import { ObservabilityModule } from '../observability/observability.module.js';
import { IngestionStatus } from '../observability/ingestion-status.service.js';
import { FlightUpdatesService } from './flight-updates.service.js';

@Module({
  imports: [ConfigurationModule, KafkaProducerModule, ObservabilityModule],
  providers: [
    {
      provide: FlightUpdatesService,
      inject: [INGESTER_CONFIG, KafkaProducerService, IngestionStatus],
      useFactory: (
        config: IngesterConfiguration,
        publisher: KafkaProducerService,
        status: IngestionStatus,
      ) => new FlightUpdatesService(config, publisher, status),
    },
  ],
})
export class ProductsModule {}
