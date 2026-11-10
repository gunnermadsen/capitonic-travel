/* Generated from events.v1.schema.json. Run npm run generate:contracts. */

/**
 * Canonical immutable v1 events. See README for ordering and boundary semantics.
 */
export type AirlineEventV1 =
  | FlightUpdateV1
  | ItineraryEventV1
  | ConnectionAssessmentV1
  | IngestionRejectionV1;
export type Uuid = string;
export type Instant = string;
export type Date = string;
export type Timezone = string;
export type Airport = string;
export type SourceRevision = number;
export type PolicyVersion = string;
export type Revision = number;
export type TransferMinutes = number;

export interface FlightUpdateV1 {
  event_id: Uuid;
  schema_version: '1.0.0';
  event_type: 'FLIGHT_UPDATED';
  event_time: Instant;
  ingested_at: Instant;
  source: string;
  operating_mode: 'SYNTHETIC' | 'PUBLIC';
  correlation_id?: Uuid;
  simulation_run_id?: Uuid;
  flight_instance_id: Uuid;
  provider_flight_id: string | null;
  operating_carrier: string;
  flight_number: string;
  service_date: Date;
  origin_timezone: Timezone;
  destination_timezone: Timezone;
  origin: Airport;
  destination: Airport;
  flight_status:
    | 'UNKNOWN'
    | 'SCHEDULED'
    | 'BOARDING'
    | 'DELAYED'
    | 'DEPARTED'
    | 'IN_AIR'
    | 'LANDED'
    | 'ARRIVED'
    | 'CANCELLED'
    | 'DIVERTED';
  scheduled_gate_arrival_at: Instant | null;
  estimated_gate_arrival_at: Instant | null;
  actual_gate_arrival_at: Instant | null;
  scheduled_gate_departure_at: Instant | null;
  estimated_gate_departure_at: Instant | null;
  boarding_cutoff_at: Instant | null;
  source_revision: SourceRevision | null;
  source_updated_at: Instant | null;
  source_timestamp_provenance: 'SOURCE_ISSUED' | 'LOCALLY_OBSERVED';
  boarding_cutoff_provenance: 'SOURCE_PROVIDED' | 'INFERRED_POLICY' | 'UNKNOWN';
  boarding_cutoff_policy_version: PolicyVersion | null;
}
export interface ItineraryEventV1 {
  event_id: Uuid;
  schema_version: '1.0.0';
  event_type: 'CREATED' | 'UPDATED' | 'CANCELLED';
  event_time: Instant;
  ingested_at: Instant;
  source: string;
  operating_mode: 'SYNTHETIC' | 'PUBLIC';
  correlation_id?: Uuid;
  simulation_run_id?: Uuid;
  itinerary_id: Uuid;
  connection_id: Uuid;
  itinerary_revision: Revision;
  inbound_flight_instance_id: Uuid;
  outbound_flight_instance_id: Uuid;
  connection_airport: Airport;
  minimum_transfer_minutes: TransferMinutes;
}
export interface ConnectionAssessmentV1 {
  event_id: Uuid;
  schema_version: '1.0.0';
  event_type: 'CONNECTION_ASSESSED';
  event_time: Instant;
  ingested_at: Instant;
  source: string;
  operating_mode: 'SYNTHETIC' | 'PUBLIC';
  correlation_id?: Uuid;
  simulation_run_id?: Uuid;
  connection_id: Uuid;
  itinerary_id: Uuid;
  revision: Revision;
  status: 'UNKNOWN' | 'ON_TRACK' | 'AT_RISK' | 'DISRUPTED';
  arrival_at_used: Instant | null;
  arrival_basis: 'ACTUAL' | 'ESTIMATED' | 'SCHEDULED' | 'UNKNOWN';
  boarding_cutoff_at_used: Instant | null;
  cutoff_basis: 'SOURCE_PROVIDED' | 'INFERRED_POLICY' | 'UNKNOWN';
  cutoff_policy_version: PolicyVersion | null;
  minimum_transfer_minutes: TransferMinutes;
  available_transfer_minutes: number | null;
  reason_code:
    | 'TIMING_UNAVAILABLE'
    | 'FLIGHT_STATE_UNAVAILABLE'
    | 'STALE_FLIGHT_DATA'
    | 'CONNECTION_AIRPORT_MISMATCH'
    | 'ITINERARY_CANCELLED'
    | 'TRANSFER_TIME_SUFFICIENT'
    | 'TRANSFER_TIME_INSUFFICIENT'
    | 'INBOUND_CANCELLED'
    | 'OUTBOUND_CANCELLED'
    | 'INBOUND_DIVERTED'
    | 'OUTBOUND_DIVERTED';
  trigger_event_id: Uuid;
  inbound_update_event_id: Uuid | null;
  outbound_update_event_id: Uuid | null;
  itinerary_event_id: Uuid;
  assessment_policy_version: PolicyVersion;
}
export interface IngestionRejectionV1 {
  event_id: Uuid;
  schema_version: '1.0.0';
  event_type: 'INGESTION_REJECTED';
  event_time: Instant;
  ingested_at: Instant;
  source: string;
  operating_mode: 'SYNTHETIC' | 'PUBLIC';
  correlation_id?: Uuid;
  simulation_run_id?: Uuid;
  data_product:
    'flight_updates' | 'itinerary_events' | 'connection_assessments';
  rejected_event_id: Uuid | null;
  payload_sha256: string;
  reason_code:
    | 'NORMALIZATION_FAILED'
    | 'SCHEMA_VALIDATION_FAILED'
    | 'UNSUPPORTED_SCHEMA_VERSION'
    | 'UNSUPPORTED_EVENT_TYPE'
    | 'PROVIDER_PAYLOAD_INVALID';
  /**
   * @minItems 1
   * @maxItems 16
   */
  issues: ValidationIssue[];
}
export interface ValidationIssue {
  path: string;
  code: string;
  message: string;
}
