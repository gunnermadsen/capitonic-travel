import {
  Injectable,
  Logger,
  type OnModuleInit,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type { Kafka, Producer } from 'kafkajs';
import { parseEvent } from '@airline/contracts';
import type { FlightUpdateV1, IngestionRejectionV1 } from '@airline/contracts';
@Injectable()
export class KafkaProducerService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly producer: Producer | undefined;
  private connected = false;
  private readonly logger = new Logger(KafkaProducerService.name);
  constructor(
    client?: Kafka,
    private readonly schemaIds?: { flight: number; rejection: number },
  ) {
    this.producer = client?.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 1,
      retry: { retries: 2, initialRetryTime: 300, maxRetryTime: 1000 },
    });
  }
  async onModuleInit(): Promise<void> {
    if (!this.producer) {
      this.logger.log({
        event: 'kafka_publication_disabled',
        output: 'validate',
      });
      return;
    }
    const mode = process.env.KAFKA_MODE ?? 'local';
    this.logger.log({ event: 'kafka_connection_check_started', mode });
    try {
      await this.producer.connect();
      this.connected = true;
      this.logger.log({ event: 'kafka_connection_check_passed', mode });
    } catch (error) {
      this.logger.error({
        event: 'kafka_connection_check_failed',
        mode,
        errorType: error instanceof Error ? error.name : 'UnknownError',
      });
      await this.producer.disconnect().catch(() => {});
      throw error;
    }
  }
  async publish(event: FlightUpdateV1 | IngestionRejectionV1): Promise<void> {
    if (!parseEvent(event).ok)
      throw new Error('Refusing invalid outgoing event');
    if (!this.producer) {
      this.logger.log({
        event: 'event_schema_validated',
        eventId: event.event_id,
        eventType: event.event_type,
        schemaVersion: event.schema_version,
        source: event.source,
      });
      return;
    }
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
    const json = JSON.stringify(event);
    let value: string | Buffer = json;
    if (this.schemaIds) {
      // Confluent JSON Schema format: magic byte, big-endian schema ID, UTF-8 JSON.
      const header = Buffer.alloc(5);
      header.writeUInt32BE(
        event.event_type === 'FLIGHT_UPDATED'
          ? this.schemaIds.flight
          : this.schemaIds.rejection,
        1,
      );
      value = Buffer.concat([header, Buffer.from(json)]);
    }
    try {
      await this.producer.send({
        topic:
          event.event_type === 'FLIGHT_UPDATED'
            ? 'flight.updates'
            : 'ingestion.rejections',
        acks: -1,
        timeout: 10000,
        messages: [
          {
            key:
              event.event_type === 'FLIGHT_UPDATED'
                ? event.flight_instance_id
                : event.event_id,
            value,
          },
        ],
      });
    } catch (error) {
      this.connected = false;
      await this.producer.disconnect().catch(() => {});
      throw error;
    }
  }
  async onApplicationShutdown(): Promise<void> {
    await this.producer?.disconnect();
    this.connected = false;
  }
}
