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
): Promise<kafkajs.Kafka> {
  const directory = options.identityDirectory;
  return new kafkajs.Kafka({
    clientId: options.clientId,
    brokers: options.brokers,
    ssl: {
      ca: [await readFile(`${directory}/ca.crt`)],
      cert: await readFile(`${directory}/tls.crt`),
      key: await readFile(`${directory}/tls.key`),
      rejectUnauthorized: true,
    },
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
