import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import { parseEventJson } from '@airline/contracts';
import { createKafkaClient } from './kafka/kafka-client.js';
const startedAt = Date.now();
const kafka = await createKafkaClient({
  clientId: 'airline-firehose-acceptance',
  brokers: [process.env.KAFKA_BROKER ?? 'localhost:19092'],
  identityDirectory: resolve('.local/identity/flink-kafka'),
});
const consumer = kafka.consumer({
  groupId: 'airline-archive-flights',
  readUncommitted: false,
  allowAutoTopicCreation: false,
});
const seen = new Map<string, string>();
let flights = 0,
  rejections = 0,
  hasGateTimes = false,
  oldestServiceDate: string | null = null,
  error: unknown;
let finish: () => void = () => {};
const completed = new Promise<void>((resolve) => {
  finish = resolve;
});
const deadline = setTimeout(() => {
  error = new Error(
    'No recent Firehose flight snapshots observed within 60 seconds',
  );
  finish();
}, 60000);
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
          result = parseEventJson(raw);
        assert.ok(result.ok);
        const event = result.value;
        if (event.source !== 'flightaware-firehose') return;
        assert.equal(event.operating_mode, 'PUBLIC');
        assert.equal('simulation_run_id' in event, false);
        const previous = seen.get(event.event_id);
        if (previous) assert.equal(previous, raw);
        seen.set(event.event_id, raw);
        if (Date.parse(event.ingested_at) < startedAt) return;
        if (
          topic === 'flight.updates' &&
          event.event_type === 'FLIGHT_UPDATED'
        ) {
          assert.equal(message.key?.toString(), event.flight_instance_id);
          assert.ok(event.provider_flight_id);
          flights++;
          hasGateTimes ||=
            event.scheduled_gate_departure_at !== null ||
            event.scheduled_gate_arrival_at !== null;
          if (
            oldestServiceDate === null ||
            event.service_date < oldestServiceDate
          )
            oldestServiceDate = event.service_date;
        } else if (event.event_type === 'INGESTION_REJECTED') {
          assert.equal(message.key?.toString(), event.event_id);
          rejections++;
        }
        if (flights >= 3 && hasGateTimes) finish();
      } catch (caught) {
        error = caught;
        finish();
      }
    },
  });
  await completed;
  if (error) throw error;
  console.log(
    JSON.stringify({
      event: 'firehose_kafka_acceptance_passed',
      flights,
      rejections,
      hasGateTimes,
      oldestServiceDate,
      consumerOffsetsCommitted: false,
    }),
  );
} finally {
  clearTimeout(deadline);
  await consumer.disconnect();
}
