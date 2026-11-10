import { readFile } from 'node:fs/promises';
import kafkajs, { type Admin, type ITopicConfig } from 'kafkajs';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

let admin: Admin | undefined;
const deadline = setTimeout(() => {
  console.error(
    JSON.stringify({ job: 'kafka-topics', event: 'deadline_exceeded' }),
  );
  process.exit(1);
}, 120000);

try {
  const manifest: unknown = JSON.parse(
    await readFile(
      process.env.TOPIC_MANIFEST ??
        new URL(
          '../../../common/configs/kafka/topics/manifest.json',
          import.meta.url,
        ),
      'utf8',
    ),
  );
  if (
    !isRecord(manifest) ||
    !Array.isArray(manifest.topics) ||
    manifest.topics.length === 0
  )
    throw new Error('Topic definitions are required');
  const topics: ITopicConfig[] = manifest.topics.map((entry: unknown) => {
    if (
      !isRecord(entry) ||
      typeof entry.name !== 'string' ||
      !entry.name ||
      typeof entry.partitions !== 'number' ||
      !Number.isInteger(entry.partitions) ||
      entry.partitions < 1 ||
      typeof entry.replicationFactor !== 'number' ||
      !Number.isInteger(entry.replicationFactor) ||
      entry.replicationFactor < 1 ||
      !isRecord(entry.config)
    )
      throw new Error('Invalid topic definition');
    return {
      topic: entry.name,
      numPartitions: entry.partitions,
      replicationFactor: entry.replicationFactor,
      configEntries: Object.entries(entry.config).map(([name, value]) => {
        if (typeof value !== 'string')
          throw new Error('Topic configuration values must be strings');
        return { name, value };
      }),
    };
  });
  const directory = process.env.IDENTITY_DIR ?? '/run/identity';
  const kafka = new kafkajs.Kafka({
    clientId: 'airline-topic-management',
    brokers: [process.env.KAFKA_BROKER ?? 'kafka:9092'],
    ssl: {
      ca: [await readFile(`${directory}/ca.crt`)],
      cert: await readFile(`${directory}/tls.crt`),
      key: await readFile(`${directory}/tls.key`),
      rejectUnauthorized: true,
    },
    connectionTimeout: 5000,
    requestTimeout: 10000,
    enforceRequestTimeout: true,
    retry: { retries: 4, initialRetryTime: 300, maxRetryTime: 3000 },
    logLevel: kafkajs.logLevel.NOTHING,
  });
  admin = kafka.admin();
  await admin.connect();
  const created = await admin.createTopics({
    topics,
    waitForLeaders: true,
    timeout: 15000,
  });
  console.log(
    JSON.stringify({
      job: 'kafka-topics',
      event: 'completed',
      created,
      topics: topics.map((topic) => topic.topic),
    }),
  );
} catch (error) {
  console.error(
    JSON.stringify({
      job: 'kafka-topics',
      event: 'failed',
      message: error instanceof Error ? error.message : 'Unknown failure',
    }),
  );
  process.exitCode = 1;
} finally {
  try {
    if (admin) await admin.disconnect();
  } catch {
    console.error(
      JSON.stringify({ job: 'kafka-topics', event: 'disconnect_failed' }),
    );
    process.exitCode = 1;
  } finally {
    clearTimeout(deadline);
  }
}
