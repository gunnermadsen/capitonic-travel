import { Module } from '@nestjs/common';
import {
  INGESTER_CONFIG,
  type IngesterConfiguration,
} from '../../configuration/ingester.configuration.js';
import { FirehoseAdapter } from './adapters/firehose.adapter.js';
import { FirehoseCheckpointStore } from './adapters/firehose-checkpoint.store.js';
import { FirehoseFlightUpdatesStrategy } from './application/firehose-flight-updates.strategy.js';
import { ConfigurationModule } from '../../configuration/configuration.module.js';
import { FileCheckpointStore } from './adapters/file-checkpoint.store.js';
import { KafkaProducerService } from '../../kafka/kafka-producer.service.js';
import { KafkaProducerModule } from '../../kafka/kafka-producer.module.js';
import { ObservabilityModule } from '../../observability/observability.module.js';
import { IngestionStatus } from '../../observability/ingestion-status.service.js';
import {
  type CheckpointStore,
  type EventPublisher,
  type FlightProvider,
} from './domain/ingestion.ports.js';
import { SimulatorAdapter } from './adapters/simulator.adapter.js';
import { FlightUpdatesStrategy } from './application/flight-updates.strategy.js';
import { FlightUpdatesWorker } from './application/flight-updates-worker.service.js';
@Module({
  imports: [ConfigurationModule, KafkaProducerModule, ObservabilityModule],
  providers: [
    {
      provide: FirehoseAdapter,
      inject: [INGESTER_CONFIG],
      useFactory: (config: IngesterConfiguration) =>
        new FirehoseAdapter(config),
    },
    {
      provide: FirehoseFlightUpdatesStrategy,
      inject: [INGESTER_CONFIG, KafkaProducerService, IngestionStatus],
      useFactory: (
        config: IngesterConfiguration,
        publisher: EventPublisher,
        status: IngestionStatus,
      ) =>
        new FirehoseFlightUpdatesStrategy(
          new FirehoseCheckpointStore(
            config.stateDirectory,
            config.firehoseAirports,
          ),
          publisher,
          status,
        ),
    },
    SimulatorAdapter,
    {
      provide: FileCheckpointStore,
      inject: [INGESTER_CONFIG],
      useFactory: (config: IngesterConfiguration) =>
        new FileCheckpointStore(config),
    },
    {
      provide: FlightUpdatesStrategy,
      inject: [
        SimulatorAdapter,
        FileCheckpointStore,
        KafkaProducerService,
        IngestionStatus,
      ],
      useFactory: (
        provider: FlightProvider,
        checkpoint: CheckpointStore,
        publisher: EventPublisher,
        status: IngestionStatus,
      ) => new FlightUpdatesStrategy(provider, checkpoint, publisher, status),
    },
    FlightUpdatesWorker,
  ],
})
export class FlightUpdatesModule {}
