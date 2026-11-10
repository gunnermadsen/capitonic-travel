import { AeroApiModule } from './products/aeroapi/aeroapi.module.js';
import { Module } from '@nestjs/common';
import { ConfigurationModule } from './configuration/configuration.module.js';
import { FlightUpdatesModule } from './products/flight-updates/flight-updates.module.js';
import { HealthModule } from './health/health.module.js';
@Module({
  imports: [
    ConfigurationModule,
    process.env.INGESTER_PRODUCT === 'aeroapi'
      ? AeroApiModule
      : FlightUpdatesModule,
    HealthModule,
  ],
})
export class AppModule {}
