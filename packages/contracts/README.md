# Airline data contracts

`@airline/contracts` owns the version 1 event contracts. The JSON Schema 2020-12
source is `src/schemas/events.v1.schema.json`. TypeScript types are generated from that source. Do not edit
`src/generated/`. All four products share the envelope definitions by reference.

## Public boundary

```ts
import { parseFlightUpdate } from '@airline/contracts';

const result = parseFlightUpdate(untrustedPayload); // unknown
if (!result.ok) {
  // Return/reject using result.issues; do not publish or persist this payload.
} else {
  // result.value is FlightUpdateV1, validated without a cast.
}
```

Use `parseFlightUpdate`, `parseItineraryEvent`, `parseConnectionAssessment`, or
`parseIngestionRejection` at producer, consumer, and HTTP boundaries. `parseEvent`
accepts any supported product; `parseEventJson` also handles malformed JSON. All
return the same discriminated result shape, apply the same validation, leave input
unchanged, and never coerce values, fill defaults, or discard unknown properties.
Errors contain at most 16 bounded issues and do not echo payload values.

Generated types describe structural shapes; runtime validation additionally
checks conditional requirements, formats, ranges, and cross-field invariants.
Direct schema users must implement the `iana-time-zone` format and the documented
cross-field checks: source event-time equality, distinct itinerary flights,
assessment duration arithmetic, and trigger-to-input references. The exported
parsers are the authoritative Node boundary implementation.

Public exports are the package root and `@airline/contracts/schemas/v1`. JSON exports require Node's JSON import attribute.

## Envelope and identity

- `schema_version` is exactly `1.0.0`; product event types are `FLIGHT_UPDATED`,
  `CREATED` / `UPDATED` / `CANCELLED`, `CONNECTION_ASSESSED`, and `INGESTION_REJECTED`.
- Identifiers are canonical lowercase UUIDs, not flight numbers. A
  `flight_instance_id` identifies one operating occurrence, including repeated
  flight numbers on different dates. Allocate it once and retain it across
  corrections, delays, provider aliases, and retries. `provider_flight_id` retains
  the provider identifier; it may be null in simulations.
- Each occurrence has one authoritative source in its operating mode. Source
  switching requires an explicit resynchronization; merging competing sources is
  not permitted. Provider normalization owns persistent identity mapping later.
- `event_id` identifies one logical event. Retries reuse the entire event,
  including IDs and timestamps. Reuse with identical payload is a duplicate;
  reuse with different payload is a conflict to reject/quarantine.
- `operating_mode` is `SYNTHETIC` or `PUBLIC`. Synthetic events require
  `simulation_run_id`; public events prohibit it. Optional `correlation_id` and
  `simulation_run_id` are omitted when inapplicable, never null.
- Kafka key contracts: flight instance for flight updates; connection ID for
  itinerary events and assessments; rejection event ID for ingestion rejections.
  These are definitions, not topic creation or ACL manifests.

## Timestamps, snapshots, and provenance

All instants use UTC `YYYY-MM-DDTHH:mm:ss.SSSZ`, exactly millisecond precision.
Offsets, missing/excess precision, invalid calendar dates, and leap seconds are
rejected. `service_date` is the original scheduled **local** operating date at the
origin; keep it stable when a flight is retimed across midnight. `origin_timezone`
and `destination_timezone` are IANA timezone identifiers (or UTC), validated
against the pinned Node runtime's timezone database. Never infer this date from
UTC arrival or ingestion time.

`ingested_at` is the first local observation, retained on retries. For flight
updates, `SOURCE_ISSUED` requires `source_updated_at` and makes it `event_time`;
`LOCALLY_OBSERVED` requires a null source timestamp and sets `event_time` to
`ingested_at`. Other products use their local logical occurrence time as
`event_time`. The two clocks need not be monotonic across producers.

Flight updates are complete normalized snapshots. Every product field is
required, including nullable timing fields, provider ID, source revision, and
policy fields. Null explicitly clears a previously known value; omission is
invalid. JSON Schema enumerates statuses and bounds; revisions are integers up to
9,007,199,254,740,991 so JSON/JavaScript never rounds them.

Arrival basis records ACTUAL, ESTIMATED, SCHEDULED, or UNKNOWN. Use actual gate
arrival before estimated before scheduled when available; missing timing remains
unknown. Boarding cutoff provenance is SOURCE_PROVIDED, INFERRED_POLICY, or
UNKNOWN. Inference requires a named policy version; unknown cutoff requires null
time and policy. Never present a scheduled departure as a provider-issued boarding
cutoff. The future assessment policy chooses inference/staleness thresholds.

## Ordering and lifecycle

Flight revisions, when supplied by the authoritative source, take precedence
over timestamps. Accept only greater revisions. When unavailable, compare
`source_updated_at` for source-issued snapshots, otherwise first observation time
`ingested_at`. The ordering basis must stay fixed for that source/flight; a switch
between revisioned and unrevisioned input, or between timestamp provenance modes,
requires explicit resynchronization. Equal ordering keys with equal normalized
flight state are duplicates; equal keys with conflicting state are rejected or
quarantined. Ignore older state for the live view while retaining input history.
Envelope observation/correlation metadata alone does not change flight state.

Itinerary events are full connection snapshots, including cancellations. CREATED
has revision 1; UPDATED and CANCELLED require revision 2 or greater. Revisions
increase per connection, not per flight or across unrelated connections in an
itinerary. A revision gap is acceptable because events are full snapshots.
CANCELLED ends that connection's active lifecycle; recreate with a new connection
ID. Inbound and outbound flight IDs must differ. Cross-event flight existence and
airport matching are later service/processor checks, not stateless schema checks.

Assessments have positive per-connection revisions, persisted with processing
state. Consumers accept only newer revisions. Recovery must restore state and
revision together; a rebuild without that state uses a new destination and must
not reset revisions into an existing live view. Event-time sorting is not a
replacement for revisions. Equal-revision conflicting payloads are errors.

Assessments retain input event IDs, trigger ID, policy version, and timing bases.
The trigger must identify a referenced input. `available_transfer_minutes` is
floor((cutoff minus arrival) / 60,000), including negative values, or null if either
time is unknown. Status/reason combinations and basis/null combinations are
validated. `ITINERARY_CANCELLED` with UNKNOWN is the inactive-connection marker;
later consumers remove it from active results while preserving history. This is
separate from a flight cancellation's DISRUPTED assessment.

Rejections retain a SHA-256 of original input bytes, an optional valid rejected
event ID, and bounded sanitized issues. Do not store raw payloads, credentials, or
provider response bodies in issues. Rejections cannot recursively target their
own data product. The provider adapter owns hashing/sanitization when introduced.

Ordering, deduplication, active state, freshness policy, and processing recovery
are specified here; implementing their stateful enforcement belongs to the later
services and Flink jobs.

## Build and verification

Run from the repository root:

```bash
npm run generate:contracts
npm run build
npm run test:contracts
npm run validate
```

The root build regenerates TypeScript types in memory and checks them against the
tracked output before compiling. `generate:contracts` writes the updated types.
Edit the JSON Schema rather than generated files. Conditional requirements remain
in the schema and are enforced by runtime validation; generated types describe the
structural shapes. Cross-field arithmetic and reference checks stay in `validation.ts`.

Tests cover all four event types, synthetic/public data, required and nullable
fields, timestamps, provenance, revisions, assessment semantics and sanitized
errors. No release-lock, schema fingerprinting or storage-mapping generator is
required. Review schema changes alongside their consumers before deployment.
