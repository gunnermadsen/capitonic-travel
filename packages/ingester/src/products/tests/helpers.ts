import { mock } from 'node:test';
import {
  configuration,
  type IngesterConfiguration,
} from '../../configuration/ingester.configuration.js';
import { IngestionStatus } from '../../observability/ingestion-status.service.js';
import {
  FlightUpdatesService,
  type AeroApiConfiguration,
  type AeroApiSource,
  type AeroApiState,
  type FirehoseState,
  type CheckpointStore,
  type FlightProvider,
  type EventPublisher,
} from '../flight-updates.service.js';

// Test fixtures exercise the service methods with controlled clocks, I/O and failures.
export function service(
  config = configuration({}),
  publisher: EventPublisher = { async publish() {} },
  status = new IngestionStatus(),
  clock = () => new Date(),
  request: typeof fetch = fetch,
  env: NodeJS.ProcessEnv = {},
) {
  return new FlightUpdatesService(
    config,
    publisher,
    status,
    clock,
    request,
    env,
  );
}
function aeroService(
  config: AeroApiConfiguration,
  publisher?: EventPublisher,
  status?: IngestionStatus,
  clock?: () => Date,
  request?: typeof fetch,
) {
  return service(
    {
      ...configuration({
        INGESTER_PRODUCT: 'aeroapi',
        INGESTER_MODE: 'PUBLIC',
        INGESTER_OUTPUT: config.output,
      }),
      stateDirectory: config.stateDirectory,
    },
    publisher,
    status,
    clock,
    request,
    {
      FLIGHTAWARE_API_KEY: config.FLIGHTAWARE_API_KEY,
      AEROAPI_AIRPORT: config.airport,
      AEROAPI_POLL_INTERVAL_MS: String(config.intervalMs),
    },
  );
}
export function createAeroApiAdapter(
  config: AeroApiConfiguration,
  request?: typeof fetch,
) {
  const s = aeroService(config, undefined, undefined, undefined, request);
  return { acquire: s.aeroAcquire.bind(s) };
}
export function createAeroApiCheckpointStore(directory: string) {
  const s = service(configuration({ INGESTER_STATE_DIR: directory }));
  return { load: s.aeroStoreLoad.bind(s), save: s.aeroStoreSave.bind(s) };
}
export function createAeroApiStrategy(
  config: AeroApiConfiguration,
  source: AeroApiSource,
  store: {
    load(): Promise<AeroApiState>;
    save(state: AeroApiState): Promise<void>;
  },
  publisher: EventPublisher,
  status: IngestionStatus,
  clock?: () => Date,
) {
  const s = aeroService(config, publisher, status, clock);
  s.aeroAcquire = source.acquire.bind(source);
  s.aeroStoreLoad = store.load.bind(store);
  s.aeroStoreSave = store.save.bind(store);
  return { initialize: s.aeroInitialize.bind(s), tick: s.aeroTick.bind(s) };
}
export function createSimulatorAdapter() {
  const s = service();
  return {
    acquire: s.simSourceAcquire.bind(s),
    normalize: s.simSourceNormalize.bind(s),
  };
}
export function createFileCheckpointStore(config: IngesterConfiguration) {
  const s = service(config);
  return { load: s.simStoreLoad.bind(s), save: s.simStoreSave.bind(s) };
}
export function createFlightUpdatesStrategy(
  provider: FlightProvider,
  store: CheckpointStore,
  publisher: EventPublisher,
  status: IngestionStatus,
  clock?: () => Date,
) {
  const s = service(undefined, publisher, status, clock);
  s.simSourceAcquire = provider.acquire.bind(provider);
  mock.method(s, 'simSourceNormalize', provider.normalize.bind(provider));
  s.simStoreLoad = store.load.bind(store);
  s.simStoreSave = store.save.bind(store);
  return { initialize: s.simInitialize.bind(s), tick: s.simTick.bind(s) };
}
export function createFirehoseCheckpointStore(
  directory: string,
  airports: string,
) {
  const s = service(
    configuration({
      INGESTER_STATE_DIR: directory,
      FIREHOSE_AIRPORTS: airports,
    }),
  );
  return {
    load: s.firehoseStoreLoad.bind(s),
    save: s.firehoseStoreSave.bind(s),
  };
}
export function createFirehoseFlightUpdatesStrategy(
  store: {
    load(): Promise<FirehoseState>;
    save(state: FirehoseState): Promise<void>;
  },
  publisher: EventPublisher,
  status: IngestionStatus,
  clock?: () => Date,
) {
  const s = service(undefined, publisher, status, clock);
  s.firehoseStoreLoad = store.load.bind(store);
  s.firehoseStoreSave = store.save.bind(store);
  return {
    initialize: s.firehoseInitialize.bind(s),
    accept: s.firehoseAccept.bind(s),
    recoveryPoint: s.firehoseRecoveryPoint.bind(s),
    flush: s.firehoseFlush.bind(s),
  };
}
export function createFirehoseFrames() {
  const s = service();
  return { push: s.framesPush.bind(s), finish: s.framesFinish.bind(s) };
}
