import { Module } from '@nestjs/common';
import { configuration, INGESTER_CONFIG } from './ingester.configuration.js';
@Module({
  providers: [
    { provide: INGESTER_CONFIG, useFactory: () => configuration(process.env) },
  ],
  exports: [INGESTER_CONFIG],
})
export class ConfigurationModule {}
