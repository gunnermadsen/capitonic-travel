import assert from 'node:assert/strict';
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
