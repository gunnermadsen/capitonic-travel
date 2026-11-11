import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { parseEvent, parseFlightUpdate } from '@airline/contracts';
import { configuration } from '../configuration/ingester.configuration.js';
import { test } from 'node:test';
import { Logger } from '@nestjs/common';
import kafkajs from 'kafkajs';
import { KafkaProducerService } from './kafka-producer.service.js';

test('startup connects the producer and logs success without publishing a test event', async (t) => {
  const client = new kafkajs.Kafka({
    clientId: 'startup-test',
    brokers: ['test.invalid:9092'],
    logLevel: kafkajs.logLevel.NOTHING,
  });
  const producer = client.producer();
  t.mock.method(client, 'producer', () => producer);
  const connect = t.mock.method(producer, 'connect', async () => {});
  const disconnect = t.mock.method(producer, 'disconnect', async () => {});
  const send = t.mock.method(producer, 'send', async () => []);
  const log = t.mock.method(Logger.prototype, 'log', () => {});
  const service = new KafkaProducerService(client);
  await service.onModuleInit();
  assert.equal(connect.mock.callCount(), 1);
  assert.equal(send.mock.callCount(), 0);
  assert.ok(
    log.mock.calls.some((call) =>
      JSON.stringify(call.arguments).includes('kafka_connection_check_passed'),
    ),
  );
  await service.onApplicationShutdown();
  assert.equal(disconnect.mock.callCount(), 1);
});

test('startup connection failure is logged, cleaned up and propagated', async (t) => {
  const client = new kafkajs.Kafka({
    clientId: 'startup-test',
    brokers: ['test.invalid:9092'],
    logLevel: kafkajs.logLevel.NOTHING,
  });
  const producer = client.producer();
  t.mock.method(client, 'producer', () => producer);
  t.mock.method(producer, 'connect', async () => {
    throw new Error('sensitive provider detail');
  });
  const disconnect = t.mock.method(producer, 'disconnect', async () => {});
  const log = t.mock.method(Logger.prototype, 'log', () => {});
  const error = t.mock.method(Logger.prototype, 'error', () => {});
  await assert.rejects(new KafkaProducerService(client).onModuleInit());
  assert.equal(disconnect.mock.callCount(), 1);
  assert.ok(
    !log.mock.calls.some((call) =>
      JSON.stringify(call.arguments).includes('kafka_connection_check_passed'),
    ),
  );
  const output = JSON.stringify(error.mock.calls.map((call) => call.arguments));
  assert.ok(output.includes('kafka_connection_check_failed'));
  assert.ok(!output.includes('sensitive provider detail'));
});

test('validation output isolates checkpoints and validates events without a Kafka client', async (t) => {
  const config = configuration({
    INGESTER_PRODUCT: 'aeroapi',
    INGESTER_MODE: 'PUBLIC',
    INGESTER_OUTPUT: 'validate',
  });
  assert.equal(config.output, 'validate');
  assert.equal(config.stateDirectory, '/var/lib/ingester/validation');
  assert.throws(() => configuration({ INGESTER_OUTPUT: 'invalid' }));
  const log = t.mock.method(Logger.prototype, 'log', () => {});
  const service = new KafkaProducerService();
  await service.onModuleInit();
  const raw: unknown = JSON.parse(
    await readFile('packages/contracts/fixtures/flight-update.json', 'utf8'),
  );
  const parsed = parseFlightUpdate(raw);
  assert.ok(parsed.ok);
  await service.publish(parsed.value);
  await assert.rejects(
    service.publish({ ...parsed.value, flight_instance_id: '' }),
  );
  await service.onApplicationShutdown();
  const output = JSON.stringify(log.mock.calls.map((call) => call.arguments));
  assert.ok(output.includes('kafka_publication_disabled'));
  assert.ok(output.includes('event_schema_validated'));
  assert.ok(!output.includes('kafka_connection_check_started'));
});

test('Confluent publication frames validated JSON with the topic schema ID; local stays JSON', async (t) => {
  const client = new kafkajs.Kafka({
    brokers: ['test.invalid:9092'],
    logLevel: kafkajs.logLevel.NOTHING,
  });
  const producer = client.producer();
  t.mock.method(client, 'producer', () => producer);
  t.mock.method(producer, 'connect', async () => {});
  t.mock.method(producer, 'disconnect', async () => {});
  const send = t.mock.method(producer, 'send', async () => []);
  for (const [file, topic, id] of [
    ['flight-update.json', 'flight.updates', 123],
    ['ingestion-rejection.json', 'ingestion.rejections', 456],
  ] as const) {
    const raw: unknown = JSON.parse(
      await readFile(`packages/contracts/fixtures/${file}`, 'utf8'),
    );
    const parsed = parseEvent(raw);
    assert.ok(parsed.ok);
    assert.ok(
      parsed.value.event_type === 'FLIGHT_UPDATED' ||
        parsed.value.event_type === 'INGESTION_REJECTED',
    );
    const service = new KafkaProducerService(client, {
      flight: 123,
      rejection: 456,
    });
    await service.onModuleInit();
    await service.publish(parsed.value);
    const record = send.mock.calls.at(-1)?.arguments[0];
    assert.equal(record?.topic, topic);
    const value = record?.messages[0]?.value;
    assert.ok(Buffer.isBuffer(value));
    assert.equal(value[0], 0);
    assert.equal(value.readUInt32BE(1), id);
    assert.deepEqual(JSON.parse(value.subarray(5).toString()), raw);
    const before = send.mock.callCount();
    await assert.rejects(service.publish({ ...parsed.value, event_id: '' }));
    assert.equal(send.mock.callCount(), before);
    await service.onApplicationShutdown();
  }
  const raw: unknown = JSON.parse(
    await readFile('packages/contracts/fixtures/flight-update.json', 'utf8'),
  );
  const parsed = parseFlightUpdate(raw);
  assert.ok(parsed.ok);
  const local = new KafkaProducerService(client);
  await local.onModuleInit();
  await local.publish(parsed.value);
  assert.equal(
    send.mock.calls.at(-1)?.arguments[0]?.messages[0]?.value,
    JSON.stringify(raw),
  );
  await local.onApplicationShutdown();
});

test('Confluent publishing requires both positive schema IDs', () => {
  assert.throws(() => configuration({ KAFKA_MODE: 'confluent' }));
  assert.throws(() =>
    configuration({
      KAFKA_MODE: 'confluent',
      KAFKA_FLIGHT_SCHEMA_ID: '123',
      KAFKA_REJECTION_SCHEMA_ID: '0',
    }),
  );
  assert.deepEqual(
    configuration({
      KAFKA_MODE: 'confluent',
      KAFKA_FLIGHT_SCHEMA_ID: '123',
      KAFKA_REJECTION_SCHEMA_ID: '456',
    }).schemaIds,
    { flight: 123, rejection: 456 },
  );
});
