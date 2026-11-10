import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import { parseEventJson } from '@airline/contracts';
import { createKafkaClient } from './kafka/kafka-client.js';
// Read-only verification: no provider requests and no Kafka offset commits.
const now = Date.now();
const kafka = await createKafkaClient({
  clientId: 'airline-aeroapi-acceptance',
  brokers: ['localhost:19092'],
  identityDirectory: resolve('.local/identity/flink-kafka'),
});
const consumer = kafka.consumer({
  groupId: 'airline-archive-flights',
  readUncommitted: false,
  allowAutoTopicCreation: false,
});
let finish: () => void = () => {};
const completed = new Promise<void>((resolve) => {
  finish = resolve;
});
let error: unknown;
const deadline = setTimeout(() => {
  error = new Error(
    'No contract-valid current AeroAPI flight found within 60 seconds',
  );
  finish();
}, 60000);
try {
  await consumer.connect();
  await consumer.subscribe({ topics: ['flight_updates'], fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      try {
        assert.ok(message.value);
        const result = parseEventJson(message.value.toString());
        assert.ok(result.ok);
        const e = result.value;
        if (
          e.source !== 'flightaware-aeroapi' ||
          e.event_type !== 'FLIGHT_UPDATED' ||
          Date.parse(e.ingested_at) < now - 900000
        )
          return;
        assert.equal(e.operating_mode, 'PUBLIC');
        assert.equal(e.source_timestamp_provenance, 'LOCALLY_OBSERVED');
        assert.equal('simulation_run_id' in e, false);
        assert.equal(message.key?.toString(), e.flight_instance_id);
        assert.ok(e.scheduled_gate_departure_at);
        assert.ok(
          Math.abs(Date.parse(e.scheduled_gate_departure_at) - now) <
            24 * 3600000,
        );
        console.log(
          JSON.stringify({
            event: 'aeroapi_kafka_acceptance_passed',
            eventId: e.event_id,
            flight: e.operating_carrier + e.flight_number,
            serviceDate: e.service_date,
            scheduledDeparture: e.scheduled_gate_departure_at,
            status: e.flight_status,
            observedAt: e.ingested_at,
            consumerOffsetsCommitted: false,
          }),
        );
        finish();
      } catch (caught) {
        error = caught;
        finish();
      }
    },
  });
  await completed;
  if (error) throw error;
} finally {
  clearTimeout(deadline);
  await consumer.disconnect();
}
