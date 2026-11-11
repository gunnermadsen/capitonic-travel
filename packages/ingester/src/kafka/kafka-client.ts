import { readFile } from 'node:fs/promises';
import kafkajs from 'kafkajs';
export interface KafkaClientOptions {
  clientId: string;
  brokers: string[];
  identityDirectory: string;
  retries?: number;
}
export async function createKafkaClient(
  options: KafkaClientOptions,
  env: NodeJS.ProcessEnv = process.env,
): Promise<kafkajs.Kafka> {
  const mode = env.KAFKA_MODE ?? 'local';
  if (mode !== 'local' && mode !== 'confluent')
    throw new Error('Invalid KAFKA_MODE');
  if (
    mode === 'confluent' &&
    (!env.KAFKA_BROKER || !env.CONFLUENT_API_KEY || !env.CONFLUENT_API_SECRET)
  )
    throw new Error(
      'KAFKA_BROKER, CONFLUENT_API_KEY and CONFLUENT_API_SECRET are required',
    );
  const sasl: kafkajs.SASLOptions | undefined =
    mode === 'confluent' && env.CONFLUENT_API_KEY && env.CONFLUENT_API_SECRET
      ? {
          mechanism: 'plain',
          username: env.CONFLUENT_API_KEY,
          password: env.CONFLUENT_API_SECRET,
        }
      : undefined;
  const directory = options.identityDirectory;
  return new kafkajs.Kafka({
    clientId: options.clientId,
    brokers: options.brokers,
    ssl:
      mode === 'confluent'
        ? true
        : {
            ca: [await readFile(`${directory}/ca.crt`)],
            cert: await readFile(`${directory}/tls.crt`),
            key: await readFile(`${directory}/tls.key`),
            rejectUnauthorized: true,
          },
    ...(sasl ? { sasl } : {}),
    connectionTimeout: 5000,
    requestTimeout: 10000,
    enforceRequestTimeout: true,
    retry: {
      initialRetryTime: 300,
      retries: options.retries ?? 4,
      maxRetryTime: 3000,
    },
    logLevel: kafkajs.logLevel.NOTHING,
  });
}
