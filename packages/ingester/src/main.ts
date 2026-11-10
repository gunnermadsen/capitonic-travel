import 'reflect-metadata';
import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
const logger = new ConsoleLogger({ json: true });
try {
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger,
    abortOnError: false,
  });
  app.enableShutdownHooks(['SIGINT', 'SIGTERM']);
} catch (error) {
  logger.error({
    event: 'ingester_startup_failed',
    errorType: error instanceof Error ? error.name : 'UnknownError',
  });
  process.exitCode = 1;
}
