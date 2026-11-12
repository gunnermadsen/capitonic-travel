# Flight ingester

For deployment inside Codex, use the [Codex Compose entrypoint](../../docs/codex-deployment.md).

A strict TypeScript NestJS worker with one flight-updates product using AeroAPI polling, Firehose streaming, or synthetic data. It publishes full
snapshots to `flight.updates` and validated, redacted failures to
`ingestion.rejections`, using its own Kafka mTLS identity and scoped ACLs.

## Organization

Use the repository's [Ingester Data Products standard](../../agents.md#ingester-data-products).
`ProductsModule` constructs one `FlightUpdatesService`. That service owns source
selection, polling/streaming, normalization, retries, checkpointing and shutdown.
Small parsing and normalization helpers live in the same file. AeroAPI is the
Compose default; Firehose and the simulator remain available through the existing
`INGESTER_PRODUCT`/`INGESTER_MODE` settings for compatibility.

- `products/products.module.ts`: registers product services with shared dependencies.
- `products/flight-updates.service.ts`: the flight-updates product and its three sources.
- `products/tests/`: behavior, failure-recovery and lifecycle tests; test-only I/O fixtures.
- `products/reference/`: airport metadata used by Firehose normalization.
- `configuration/`, `kafka/`, `health/`, `observability/`: shared infrastructure.

The [AeroAPI source notes](src/products/aeroapi.md) document polling semantics.
Checkpoint filenames and formats, Kafka topics, event identities and secret names
are unchanged. Only the selected source starts in each process.

## Run

Run `npm ci` and `npm run validate` at the repository root with the pinned Node/npm
versions. Existing infrastructure and initialized topics must be available. Root `.env`
provides `FLIGHTAWARE_API_KEY` for AeroAPI, or `FIREHOSE_USERNAME` and
`FIREHOSE_API_KEY` for Firehose, unchanged through Compose and configuration.
`.env` is ignored; never commit or print it. No additional dependencies are required.
Stage the existing identity without replacing CA or certificate material:

Provision `.local/runtime/ingester` from the existing identity material
using the file mappings in `common/configs/identities/runtime.json`.

The container runs as UID/GID 1000. Ensure the staged PEM files are readable by that
identity and the dedicated `airline-dev_ingester-state` Docker volume is writable
by it before startup. Only the ingester certificate, key and CA are mounted.

```sh
docker compose -f docker-compose.local.yml up -d
# Or: docker compose -f docker-compose.confluent.yml up -d
docker compose -f docker-compose.local.yml stop ingester
```

The local Compose overlay starts Kafka and the existing topic job as dependencies.
The Confluent overlay runs only the ingester, using pre-provisioned topics and TLS/SASL credentials
from `CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET`, connecting to `KAFKA_BROKER`.
See [deployment commands](../../docs/local-development.md#choose-local-infrastructure-or-confluent-cloud). It adds no
readiness coordinator. The worker runs read-only except for its state volume and
`/tmp`; an OS `flock` prevents concurrent writers to the same state. The service
uses 256 MiB and 0.25 CPU, with a 45-second shutdown grace period.

| Environment                       | Default                                                | Purpose                                    |
| --------------------------------- | ------------------------------------------------------ | ------------------------------------------ |
| `INGESTER_PRODUCT`                | `aeroapi` in Compose; `flight-updates` outside Compose | Select independent product                 |
| `FLIGHTAWARE_API_KEY`             | Required for AeroAPI                                   | Exact `.env` AeroAPI secret key            |
| `INGESTER_MODE`                   | `PUBLIC` in Compose; `SYNTHETIC` outside Compose       | Public source or simulator                 |
| `FIREHOSE_USERNAME`               | Required for Firehose                                  | Exact `.env` username key                  |
| `FIREHOSE_API_KEY`                | Required for Firehose                                  | Exact `.env` Firehose secret key           |
| `FIREHOSE_AIRPORTS`               | `KJFK`                                                 | Space-separated ICAO airports, at most ten |
| `KAFKA_BROKER`                    | `kafka:9092`                                           | Broker with TLS hostname verification      |
| `IDENTITY_DIR`                    | `/run/identity`                                        | Scoped PEM identity                        |
| `INGESTER_STATE_DIR`              | `/var/lib/ingester`                                    | Persistent cursor and pending event        |
| `INGESTER_INTERVAL_MS`            | `1000`                                                 | Delay between sequential attempts          |
| `INGESTER_ACQUISITION_TIMEOUT_MS` | `5000`                                                 | Provider acquisition timeout               |
| `SIMULATION_RUN_ID`               | Persisted generated UUID                               | Optional deterministic run                 |
| `SIMULATION_START_AT`             | Persisted initial UTC instant                          | Optional UTC millisecond timestamp         |

Existing state wins; conflicting explicit run/start configuration or corrupt state
fails startup. Do not delete the volume to recover an interrupted publication.

## Delivery and recovery

Each simulator cycle emits scheduled flights, an inbound delay, an outbound delay,
and an inbound cancellation. Flight identity, revisions, timestamp provenance and
inferred boarding-cutoff policy are explicit. Simulator source time is a logical
clock; freshness metrics distinguish it from local publication time.

The exact event is saved before publication. Progress advances only after Kafka
acknowledges the event and the updated checkpoint is durable. A lost acknowledgement
or crash after publication may replay an identical event ID and payload. Delivery
is at least once; downstream consumers must deduplicate. Kafka producer idempotency
does not make this multi-system workflow exactly once. Scaling to multiple workers
or sharing a cursor across hosts is outside this single-worker implementation.

Acquisition and Kafka requests have timeouts, publication retries are bounded,
and worker retries use capped backoff. Shutdown stops acquisition, drains the
active attempt, and closes the producer. Invalid payloads become schema-validated
rejections with a payload digest, not raw provider data. Firehose uses a persistent TLS stream rather than the simulator polling interval.

## Firehose contract compatibility and recovery

The adapter connects to `firehose.flightaware.com:1501`, verifies the server
certificate/hostname, and requests `flifo`, `departure`, `arrival`, and
`cancellation` for airline flights touching the configured airports. It requests
15-second keepalives, disconnects after 90 seconds without incoming bytes, and
uses the worker's capped retry backoff. Frames are limited to 1 MiB. Sequential
socket iteration applies backpressure while publication and checkpointing finish.
No WebSocket server, extra service, or paid AeroAPI enrichment calls are used.

- Firehose `id` maps to the retained provider ID and a stable canonical UUID.
- `orig`/`dest` are enriched from a pinned, MIT-licensed airport reference to
  IATA codes and IANA timezones. Unknown airports are rejected, not guessed.
- The initial observed `scheduled_out` establishes the local service date;
  corrections do not change that date or flight identity. When joining a flight
  midstream this is the earliest schedule observed, not proof of its original
  airline schedule. A missing scheduled departure defers valid publication.
- Scheduled/estimated/actual `in` times map to gate arrival; scheduled/estimated
  `out` times map to gate departure. Epoch seconds become UTC milliseconds.
  Runway `on`/`off` times influence status only. Foresight predictions are not
  substituted for estimated gate times because v1 has no separate prediction
  provenance. Boarding cutoff remains null with UNKNOWN provenance.
- Source revision and source update time remain null. `pitr` is a recovery
  cursor, not an authoritative flight-update timestamp. Events use explicit
  LOCALLY_OBSERVED provenance and monotonically increasing local observation times.
- Omitted fields retain prior state; explicit null clears a value. Full normalized
  snapshots pass the existing `parseFlightUpdate` validator before Kafka send.
  Incomplete/invalid flight records produce redacted canonical rejections while
  recoverable partial state is retained. Malformed protocol frames or messages
  without a recoverable flight identity stop/retry without advancing progress.
- Confirmed cancellations produce CANCELLED; presumed cancellations remain UNKNOWN.
  Uncancel clears cancellation. Gate arrival and runway landing remain distinct.
  Diversion is not inferred from an airport change without an authoritative signal.

Public state lives in `flight-updates-firehose.json`, separately from the unchanged
simulator checkpoint. Partial state, deduplication keys, and the pending event are
fsynced before publication; the recovery cursor advances only after acknowledgement
and a durable commit. Restart replays any pending event byte-for-byte, then requests
`pitr` recovery. Older per-flight cursors and exact same-cursor duplicates are
ignored; distinct updates sharing a cursor are processed. Unchanged normalized
state does not generate another flight event. Rejections retain replay identities.
No database migration or canonical schema change is needed.

Changing the airport subscription with existing public state fails closed. State
is bounded to 5,000 flights and 10,000 distinct updates per flight/cursor; reaching
a bound stops progress rather than discarding recovery history. Automatic state
expiry is not implemented. Monitor volume growth for longer runs. Provider replay
availability bounds outage recovery; there is no silent fallback to `live` if
recovery is refused. Source-issued ordering/corrections cannot be reconstructed
when the feed does not supply those timestamps; transport order is retained.

Use `INGESTER_PRODUCT=flight-updates INGESTER_MODE=SYNTHETIC docker compose -f docker-compose.local.yml up -d ingester` to return to the existing
simulator run. The Firehose secrets keep the same names in all modes.

Official references: [connection protocol](https://www.flightaware.com/commercial/firehose/documentation/connection),
[subscription commands](https://www.flightaware.com/commercial/firehose/documentation/commands),
[message fields](https://www.flightaware.com/commercial/firehose/documentation/messages).

## Verification and operations

```sh
npm run test:ingester
npm run verify:ingester
npm run verify:firehose
docker exec airline-dev-ingester-1 node packages/ingester/dist/health/probe.js live
docker exec airline-dev-ingester-1 node packages/ingester/dist/health/probe.js ready
docker exec airline-dev-ingester-1 node packages/ingester/dist/health/probe.js metrics
```

Compose uses the readiness probe. Probes reject stale heartbeat files; readiness
requires recent publication in synthetic mode; PUBLIC readiness requires
initialized processing, successful handling, and a recent stream message.
Inspect publication/rejection counters separately from stream readiness. Metrics include pending
and committed event IDs, run ID, cursor, publication/rejection/failure counts and
freshness. No network health endpoint is exposed.

The integration check requires the running simulator. It uses the existing
`flink-kafka` verification identity and `airline-archive-flights` group, commits no
consumer offsets, and must run before that archive group is used by a Flink job.
It validates real flight records and publishes one deliberately invalid synthetic
input as a rejection under a unique run ID; that record follows normal retention.

Focused tests cover stable replay after lost acknowledgements, checkpoint failure,
invalid payload handling, provider timeout, configuration and corrupt state.
Local evidence is kept under ignored `.local/evidence/ingester-*` files.

Verified locally on 2026-11-08: clean root installation with no nested dependencies,
full root validation, seven focused ingestion tests, and real Kafka publication
and rejection checks. Network isolation retained cursor 215 and pending event
`d7e00e16-f666-51c2-ac62-48bb4cd486b8`; recreation published that event first and
advanced the same simulation run. Readiness failed during the outage and recovered;
liveness remained available. A second worker exited with status 1 on lock conflict.
SIGTERM shutdown completed within Compose's grace period (exit 143, without forced
SIGKILL). The recreated container reported healthy. These checks do not verify
Flink assessments or a public-data adapter, which are not part of this service.

The Firehose acceptance check reads recently ingested PUBLIC events from Kafka,
validates contracts/keys, requires gate-time coverage, and reports operational
service dates. It uses the same verification consumer identity/group as the
synthetic check, without committing offsets. Do not run either verification
consumer concurrently with a future Flink job using that group.

The activated trial delivered records with January 2020 flight dates during the
2026-11-08 integration session. Those dates are preserved. A healthy connection
and recent ingestion time do not establish that the provider is serving current
flights; confirm the trial dataset with FlightAware before using it for current
connection-risk decisions.

Firehose verification on 2026-11-08: root validation and 19 ingester tests passed.
The live Kafka check observed three fresh contract-valid PUBLIC snapshots with gate
times; the post-restart check independently observed three new snapshots. The
checkpoint cursor advanced after recovery, duplicate messages were suppressed,
and the synthetic checkpoint retained its exact SHA-256. A transient provider
connection-slot limit delayed reconnection; bounded backoff recovered without
resetting the cursor. The final worker reported ready/healthy. Evidence is under
`.local/evidence/firehose-*`. Many trial records lack enough schedule metadata to
form full snapshots; they are reported as rejections while partial state is retained.
