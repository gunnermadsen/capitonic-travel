import 'reflect-metadata';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { createKafkaClient } from './kafka/kafka-client.js';
import { parseEventJson } from '@airline/contracts';
import { KafkaProducerService } from './kafka/kafka-producer.service.js';
import { FlightUpdatesService } from './products/flight-updates.service.js';
import { configuration } from './configuration/ingester.configuration.js';
import { IngestionStatus } from './observability/ingestion-status.service.js';
const directory = await mkdtemp(join(tmpdir(), 'airline-ingester-acceptance-'));
const runId = randomUUID(),
  observed = new Set<string>(),
  payloads = new Map<string, string>();
const kafka = await createKafkaClient({
  clientId: 'airline-ingester-acceptance',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:19092'],
  identityDirectory: resolve('.local/identity/flink-kafka'),
});
const consumer = kafka.consumer({
  groupId: 'airline-archive-flights',
  readUncommitted: false,
  allowAutoTopicCreation: false,
});
const producer = new KafkaProducerService(
  await createKafkaClient({
    clientId: 'airline-ingester-acceptance-write',
    brokers: [process.env.KAFKA_BROKER ?? 'localhost:19092'],
    identityDirectory: resolve('.local/identity/ingester'),
    retries: 2,
  }),
);
let rejectObserved = false;
let finish: () => void = () => {};
let fail: (error: unknown) => void = () => {};
const completed = new Promise<void>((resolve, reject) => {
  finish = resolve;
  fail = reject;
});
const deadline = setTimeout(
  () => fail(new Error('Kafka acceptance timed out')),
  45000,
);
try {
  await consumer.connect();
  await consumer.subscribe({
    topics: ['flight.updates', 'ingestion.rejections'],
    fromBeginning: true,
  });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message, topic }) => {
      try {
        assert.ok(message.value);
        const raw = message.value.toString('utf8'),
          parsed = parseEventJson(raw);
        assert.ok(parsed.ok);
        const event = parsed.value;
        if (topic === 'flight.updates') {
          assert.equal(event.event_type, 'FLIGHT_UPDATED');
          if (event.event_type === 'FLIGHT_UPDATED') {
            assert.equal(message.key?.toString(), event.flight_instance_id);
            observed.add(event.flight_status);
            const previous = payloads.get(event.event_id);
            if (previous) assert.equal(previous, raw);
            payloads.set(event.event_id, raw);
          }
        } else if (
          event.event_type === 'INGESTION_REJECTED' &&
          event.simulation_run_id === runId
        ) {
          assert.equal(event.reason_code, 'NORMALIZATION_FAILED');
          assert.equal(message.key?.toString(), event.event_id);
          rejectObserved = true;
        }
        if (
          observed.has('CANCELLED') &&
          observed.has('DELAYED') &&
          observed.has('SCHEDULED') &&
          rejectObserved
        )
          finish();
      } catch (error) {
        fail(error);
      }
    },
  });
  const status = new IngestionStatus();
  const service = new FlightUpdatesService(
    configuration({
      INGESTER_STATE_DIR: directory,
      IDENTITY_DIR: resolve('.local/identity/ingester'),
      SIMULATION_RUN_ID: runId,
    }),
    producer,
    status,
  );
  service.simSourceAcquire = async () => Buffer.from('{"invalid":true}');
  service.simSourceNormalize = () => {
    throw new Error('Invalid test snapshot');
  };
  await service.simInitialize();
  await service.simTick(new AbortController().signal);
  await completed;
  assert.equal(status.rejected, 1);
  assert.equal((await service.simStoreLoad()).nextSequence, 1);
  console.log(
    JSON.stringify({
      event: 'ingester_kafka_acceptance_passed',
      validatedFlightEvents: payloads.size,
      statuses: [...observed],
      rejectionRunId: runId,
      consumerOffsetsCommitted: false,
    }),
  );
} finally {
  clearTimeout(deadline);
  await consumer.disconnect();
  await producer.onApplicationShutdown();
  await rm(directory, { recursive: true, force: true });
}
