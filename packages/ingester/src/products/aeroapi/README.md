# AeroAPI data product

A separate Nest module, polling worker, strategy, provider contract and checkpoint,
selected with `INGESTER_PRODUCT=aeroapi` and `INGESTER_MODE=PUBLIC`. This is the
Compose default. The existing streaming product is selected with
`INGESTER_PRODUCT=flight-updates` and `INGESTER_MODE=PUBLIC`; its simulator uses
`INGESTER_MODE=SYNTHETIC`. Only the selected product starts in each process.

`FLIGHTAWARE_API_KEY` is read under that exact name from root `.env` by
`docker compose -f docker-compose.local.yml up -d ingester`. The HTTP `x-apikey` header is AeroAPI's required wire
protocol; there is no alternate secret configuration name.

The independent provider boundary follows FlightAware's
[OpenAPI specification](https://www.flightaware.com/commercial/aeroapi/resources/aeroapi-openapi.yml).
It does not import Firehose implementation code. Both products publish canonical
`FlightUpdateV1` full snapshots, validated by `@airline/contracts`, to
`flight.updates`, keyed by `flight_instance_id`. Provider-specific differences
stay inside this product. Invalid, incomplete, out-of-window or ambiguous flights
produce `IngestionRejectionV1` on `ingestion.rejections`. Its `data_product` field
identifies the canonical output `flight_updates`.

## Usage limits

- `AEROAPI_FLIGHTS=AAL1,DAL1`: one to four unique ICAO designators.
- `AEROAPI_POLL_INTERVAL_MS=300000`: one HTTP attempt every five minutes,
  round-robin across designators (ten minutes per flight with two configured).
- `AEROAPI_MAX_REQUESTS=12`: total lifetime attempts for this checkpoint, including
  failed requests. No daily or restart reset. Raising this explicitly adds quota;
  never delete the checkpoint to reset usage or event identity.
- `GET /flights/{ident}` only, `max_pages=1`, no pagination, no enrichment calls.
  Query window is current time minus 18 hours through plus 6 hours.
- Fifteen-second HTTP timeout, one MiB response cap; no adapter-level retry.
  Worker retries on the next scheduled attempt, honoring longer `Retry-After`.
  HTTP 401/403 halt durably. Resolve credentials/entitlement before an operator
  clears only `haltReason` in the stopped worker's checkpoint; retain the count.
- A request limit is not an account billing limit. Account subscription charges
  and usage outside this process are independent.

## Identity, recovery and semantics

`/var/lib/ingester/aeroapi.json` has an independent state namespace. The container's
existing process lock serializes access. Each attempt is reserved and fsynced
before HTTP; polling deadlines survive restart. Pending publications are persisted
before Kafka and removed only after acknowledgement. Retries replay exact event
IDs. Unchanged material snapshots are suppressed. Provider IDs have an AeroAPI
UUID namespace, distinct from Firehose; switching sources is not identity merging.
Do not combine both sources as authoritative state for one itinerary flight.

Gate fields map only from `*_out`/`*_in`; runway times only help determine status.
Missing optional times remain null. Scheduled origin-local service date is retained
across corrections. No provider revision or source-issued update time is invented:
observations use monotonic local times and `LOCALLY_OBSERVED`. Boarding cutoff is
unknown. A cancellation tracking flag without an explicit cancelled status is
`UNKNOWN`. Repeated provider IDs in one response (including diversion legs) are
rejected instead of overwriting each other. Polling observes snapshots; intermediate
changes between polls and upstream stale snapshots cannot be recovered or ordered
without provider revision information.

Health metrics expose product, usage, budget exhaustion, halt reason, next poll,
rejections and publication timestamps. A successful empty or unchanged response
counts as acquisition health, not a newly published flight. Readiness expires after
two poll intervals plus 30 seconds without a successful response. The process stays
alive when budget is exhausted and performs no further HTTP calls.

`npm run test:ingester` covers provider normalization, quota/restart behavior,
Retry-After, authorization halt, contract validation and Kafka acknowledgement
replay. `npm run verify:aeroapi` consumes recent current-date AeroAPI records from
Kafka without calling FlightAware or committing offsets. The acceptance consumer
uses the existing archive group; run it only while archival consumers are stopped.

Live verification on 2026-11-09 at 04:48 UTC observed DAL1, JFK–LHR, IN_AIR,
service date 2026-11-08 (origin-local), scheduled departure 01:00 UTC and estimated
gate arrival 07:55 UTC. The normalized snapshot passed the canonical validator and
was read back from Kafka with the correct key. Two HTTP attempts were consumed,
including an initial unsuccessful attempt; whole-second request timestamps were
used for the successful poll. This verifies current data ingestion, not an observed
delay transition or the provider's underlying update latency.
