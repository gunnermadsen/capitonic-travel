import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createKafkaClient } from './kafka-client.js';

const options = {
  clientId: 'configuration-test',
  brokers: ['example.invalid:9092'],
  identityDirectory: '/nonexistent-airline-test-identity',
};
test('Confluent client requires exact secret names and does not load local certificates', async () => {
  const env = {
    KAFKA_MODE: 'confluent',
    KAFKA_BROKER: 'example.invalid:9092',
    CONFLUENT_API_KEY: 'test-key',
    CONFLUENT_API_SECRET: 'test-secret',
  };
  const client = await createKafkaClient(options, env);
  assert.ok(client);
  await assert.rejects(
    createKafkaClient(options, { ...env, CONFLUENT_API_KEY: '' }),
    /CONFLUENT_API_KEY/,
  );
  await assert.rejects(
    createKafkaClient(options, { ...env, CONFLUENT_API_SECRET: '' }),
    /CONFLUENT_API_SECRET/,
  );
  await assert.rejects(
    createKafkaClient(options, { ...env, KAFKA_BROKER: '' }),
    /KAFKA_BROKER/,
  );
});
test('Local mode still requires certificate files and unknown modes fail closed', async () => {
  await assert.rejects(createKafkaClient(options, {}), /ENOENT/);
  await assert.rejects(
    createKafkaClient(options, { KAFKA_MODE: 'invalid' }),
    /Invalid KAFKA_MODE/,
  );
});
