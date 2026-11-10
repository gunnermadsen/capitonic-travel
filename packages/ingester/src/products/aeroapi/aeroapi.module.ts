import { Module } from '@nestjs/common';
import { ConfigurationModule } from '../../configuration/configuration.module.js';
import {
  INGESTER_CONFIG,
  type IngesterConfiguration,
} from '../../configuration/ingester.configuration.js';
import { ObservabilityModule } from '../../observability/observability.module.js';
import { IngestionStatus } from '../../observability/ingestion-status.service.js';
import { KafkaProducerModule } from '../../kafka/kafka-producer.module.js';
import { KafkaProducerService } from '../../kafka/kafka-producer.service.js';
import {
  AEROAPI_CONFIG,
  aeroApiConfiguration,
  type AeroApiConfiguration,
} from './aeroapi.configuration.js';
import { AeroApiAdapter } from './adapters/aeroapi.adapter.js';
import { AeroApiCheckpointStore } from './adapters/aeroapi-checkpoint.store.js';
import { AeroApiStrategy } from './application/aeroapi.strategy.js';
import { AeroApiWorker } from './application/aeroapi-worker.service.js';
@Module({
  imports: [ConfigurationModule, ObservabilityModule, KafkaProducerModule],
  providers: [
    {
      provide: AEROAPI_CONFIG,
      inject: [INGESTER_CONFIG],
      useFactory: (config: IngesterConfiguration) =>
        aeroApiConfiguration(process.env, config.stateDirectory),
    },
    {
      provide: AeroApiAdapter,
      inject: [AEROAPI_CONFIG],
      useFactory: (config: AeroApiConfiguration) => new AeroApiAdapter(config),
    },
    {
      provide: AeroApiCheckpointStore,
      inject: [AEROAPI_CONFIG],
      useFactory: (config: AeroApiConfiguration) =>
        new AeroApiCheckpointStore(config.stateDirectory),
    },
    {
      provide: AeroApiStrategy,
      inject: [
        AEROAPI_CONFIG,
        AeroApiAdapter,
        AeroApiCheckpointStore,
        KafkaProducerService,
        IngestionStatus,
      ],
      useFactory: (
        config: AeroApiConfiguration,
        adapter: AeroApiAdapter,
        store: AeroApiCheckpointStore,
        publisher: KafkaProducerService,
        status: IngestionStatus,
      ) => new AeroApiStrategy(config, adapter, store, publisher, status),
    },
    AeroApiWorker,
  ],
})
export class AeroApiModule {}
