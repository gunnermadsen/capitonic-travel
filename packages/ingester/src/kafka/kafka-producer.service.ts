import { Injectable, type OnApplicationShutdown } from '@nestjs/common';
import type { Kafka, Producer } from 'kafkajs';
import { parseEvent } from '@airline/contracts';
import type { FlightUpdateV1, IngestionRejectionV1 } from '@airline/contracts';
@Injectable()
export class KafkaProducerService implements OnApplicationShutdown {
  private readonly producer: Producer;
  private connected = false;
  constructor(client: Kafka) {
    this.producer = client.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 1,
      retry: { retries: 2, initialRetryTime: 300, maxRetryTime: 1000 },
    });
  }
  async publish(event: FlightUpdateV1 | IngestionRejectionV1): Promise<void> {
    if (!parseEvent(event).ok)
      throw new Error('Refusing invalid outgoing event');
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
    try {
      await this.producer.send({
        topic:
          event.event_type === 'FLIGHT_UPDATED'
            ? 'flight_updates'
            : 'ingestion_rejections',
        acks: -1,
        timeout: 10000,
        messages: [
          {
            key:
              event.event_type === 'FLIGHT_UPDATED'
                ? event.flight_instance_id
                : event.event_id,
            value: JSON.stringify(event),
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
    await this.producer.disconnect();
    this.connected = false;
  }
}
