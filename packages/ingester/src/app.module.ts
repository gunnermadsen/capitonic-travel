import { Module } from '@nestjs/common';
import { ProductsModule } from './products/products.module.js';
import { HealthModule } from './health/health.module.js';
@Module({ imports: [ProductsModule, HealthModule] })
export class AppModule {}
