import { Module } from '@nestjs/common';
import { ConfigurationModule } from '../configuration/configuration.module.js';
import {
  INGESTER_CONFIG,
  type IngesterConfiguration,
} from '../configuration/ingester.configuration.js';
import { createKafkaClient } from './kafka-client.js';
import { KafkaProducerService } from './kafka-producer.service.js';

@Module({
  imports: [ConfigurationModule],
  providers: [
    {
      provide: KafkaProducerService,
      inject: [INGESTER_CONFIG],
      useFactory: async (config: IngesterConfiguration) =>
        new KafkaProducerService(
          await createKafkaClient({
            clientId: 'airline-ingester',
            brokers: [config.broker],
            identityDirectory: config.identityDirectory,
            retries: 2,
          }),
        ),
    },
  ],
  exports: [KafkaProducerService],
})
export class KafkaProducerModule {}
