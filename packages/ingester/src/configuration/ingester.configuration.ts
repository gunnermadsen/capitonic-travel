import { isAbsolute } from 'node:path';
export const INGESTER_CONFIG = Symbol('INGESTER_CONFIG');
export interface IngesterConfiguration {
  product: 'flight-updates' | 'aeroapi';
  mode: 'SYNTHETIC' | 'PUBLIC';
  FIREHOSE_USERNAME: string;
  FIREHOSE_API_KEY: string;
  firehoseAirports: string;
  broker: string;
  identityDirectory: string;
  stateDirectory: string;
  intervalMs: number;
  acquisitionTimeoutMs: number;
  runId: string | undefined;
  startAt: string | undefined;
}
export function configuration(
  env: NodeJS.ProcessEnv,
): Readonly<IngesterConfiguration> {
  const integer = (
    name: string,
    fallback: number,
    min: number,
    max: number,
  ): number => {
    const raw = env[name] ?? String(fallback);
    if (!/^\d+$/.test(raw)) throw new Error(`Invalid ${name}`);
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < min || value > max)
      throw new Error(`Invalid ${name}`);
    return value;
  };
  const product = env.INGESTER_PRODUCT ?? 'flight-updates';
  if (product !== 'flight-updates' && product !== 'aeroapi')
    throw new Error('Invalid INGESTER_PRODUCT');
  const mode = env.INGESTER_MODE ?? 'SYNTHETIC';
  if (mode !== 'PUBLIC' && mode !== 'SYNTHETIC')
    throw new Error('Invalid INGESTER_MODE');
  if (product === 'aeroapi' && mode !== 'PUBLIC')
    throw new Error('AeroAPI requires PUBLIC mode');
  if (
    product === 'flight-updates' &&
    mode === 'PUBLIC' &&
    (!env.FIREHOSE_USERNAME ||
      !env.FIREHOSE_API_KEY ||
      !/^[A-Za-z0-9_.@+/=~-]+$/.test(env.FIREHOSE_USERNAME) ||
      !/^[A-Za-z0-9_.@+/=~-]+$/.test(env.FIREHOSE_API_KEY))
  )
    throw new Error(
      'Valid FIREHOSE_USERNAME and FIREHOSE_API_KEY are required',
    );
  const firehoseAirports = env.FIREHOSE_AIRPORTS ?? 'KJFK';
  if (!/^[A-Z]{4}(?: [A-Z]{4}){0,9}$/.test(firehoseAirports))
    throw new Error('Invalid FIREHOSE_AIRPORTS');
  if (mode === 'PUBLIC' && (env.SIMULATION_RUN_ID || env.SIMULATION_START_AT))
    throw new Error('Public mode cannot use simulation configuration');
  const runId = env.SIMULATION_RUN_ID,
    startAt = env.SIMULATION_START_AT;
  if (
    runId &&
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      runId,
    )
  )
    throw new Error('Invalid SIMULATION_RUN_ID');
  if (
    startAt &&
    (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(startAt) ||
      !Number.isFinite(Date.parse(startAt)) ||
      new Date(startAt).toISOString() !== startAt)
  )
    throw new Error('Invalid SIMULATION_START_AT');
  const identityDirectory = env.IDENTITY_DIR ?? '/run/identity',
    stateDirectory = env.INGESTER_STATE_DIR ?? '/var/lib/ingester';
  if (!isAbsolute(identityDirectory) || !isAbsolute(stateDirectory))
    throw new Error('Identity and state directories must be absolute');
  const broker = env.KAFKA_BROKER ?? 'kafka:9092';
  if (!/^[a-zA-Z0-9.-]+:\d+$/.test(broker))
    throw new Error('Invalid KAFKA_BROKER');
  return Object.freeze({
    product,
    mode,
    FIREHOSE_USERNAME: env.FIREHOSE_USERNAME ?? '',
    FIREHOSE_API_KEY: env.FIREHOSE_API_KEY ?? '',
    firehoseAirports,
    broker,
    identityDirectory,
    stateDirectory,
    runId,
    startAt,
    intervalMs: integer('INGESTER_INTERVAL_MS', 1000, 100, 60000),
    acquisitionTimeoutMs: integer(
      'INGESTER_ACQUISITION_TIMEOUT_MS',
      5000,
      100,
      30000,
    ),
  });
}
