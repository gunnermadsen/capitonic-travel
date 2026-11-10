import { Ajv2020, type ValidateFunction } from 'ajv/dist/2020.js';
import { fullFormats } from 'ajv-formats/dist/formats.js';
import schema from './schemas/events.v1.schema.json' with { type: 'json' };
import type {
  AirlineEventV1,
  FlightUpdateV1,
  ItineraryEventV1,
  ConnectionAssessmentV1,
  IngestionRejectionV1,
  ValidationIssue,
} from './generated/events.v1.js';

export type ValidationResult<T> =
  { ok: true; value: T } | { ok: false; issues: ValidationIssue[] };
const ajv = new Ajv2020({
  strict: true,
  strictTypes: false,
  strictRequired: false,
  allErrors: true,
  coerceTypes: false,
  useDefaults: false,
  removeAdditional: false,
});
ajv.addFormat('date', fullFormats.date);
ajv.addFormat('date-time', fullFormats['date-time']);
ajv.addFormat('uuid', fullFormats.uuid);
ajv.addFormat('iana-time-zone', {
  type: 'string',
  validate: (value: string): boolean => {
    if (value !== 'UTC' && !/^[A-Za-z_+-]+(?:\/[A-Za-z0-9_+-]+)+$/.test(value))
      return false;
    try {
      new Intl.DateTimeFormat('en', { timeZone: value });
      return true;
    } catch {
      return false;
    }
  },
});
ajv.addSchema(schema);
function compile<T>(definition: string): ValidateFunction<T> {
  return ajv.compile<T>({ $ref: `${schema.$id}#/$defs/${definition}` });
}
const flight = compile<FlightUpdateV1>('flightUpdate');
const itinerary = compile<ItineraryEventV1>('itineraryEvent');
const assessment = compile<ConnectionAssessmentV1>('connectionAssessment');
const rejection = compile<IngestionRejectionV1>('ingestionRejection');
const event = ajv.compile<AirlineEventV1>({ $ref: schema.$id });
function issue(path: string, code: string, message: string): ValidationIssue {
  return { path, code, message };
}
function semantics(value: AirlineEventV1): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  if (value.event_type === 'FLIGHT_UPDATED') {
    const expected =
      value.source_timestamp_provenance === 'SOURCE_ISSUED'
        ? value.source_updated_at
        : value.ingested_at;
    if (value.event_time !== expected)
      issues.push(
        issue(
          '/event_time',
          'TIMESTAMP_PROVENANCE',
          'Event time must match its declared source timestamp provenance.',
        ),
      );
  } else if (value.event_type === 'CONNECTION_ASSESSED') {
    const expected =
      value.arrival_at_used === null || value.boarding_cutoff_at_used === null
        ? null
        : Math.floor(
            (Date.parse(value.boarding_cutoff_at_used) -
              Date.parse(value.arrival_at_used)) /
              60000,
          );
    if (value.available_transfer_minutes !== expected)
      issues.push(
        issue(
          '/available_transfer_minutes',
          'TRANSFER_DURATION',
          'Transfer minutes must equal the floored cutoff-minus-arrival duration, or null when timing is unavailable.',
        ),
      );
    if (
      ![
        value.inbound_update_event_id,
        value.outbound_update_event_id,
        value.itinerary_event_id,
      ].includes(value.trigger_event_id)
    )
      issues.push(
        issue(
          '/trigger_event_id',
          'INPUT_REFERENCE',
          'Trigger must identify one of the referenced input events.',
        ),
      );
  } else if (value.event_type !== 'INGESTION_REJECTED') {
    if (value.inbound_flight_instance_id === value.outbound_flight_instance_id)
      issues.push(
        issue(
          '/outbound_flight_instance_id',
          'DISTINCT_FLIGHTS',
          'Inbound and outbound flight instances must differ.',
        ),
      );
  }
  return issues;
}
function validate<T extends AirlineEventV1>(
  check: ValidateFunction<T>,
  input: unknown,
): ValidationResult<T> {
  if (!check(input))
    return {
      ok: false,
      issues: (check.errors ?? [])
        .slice(0, 16)
        .map((error) =>
          issue(
            error.instancePath.length <= 256 ? error.instancePath : '',
            'SCHEMA_VALIDATION',
            `Contract constraint failed: ${error.keyword}.`,
          ),
        ),
    };
  const issues = semantics(input);
  return issues.length === 0
    ? { ok: true, value: input }
    : { ok: false, issues };
}
export const parseFlightUpdate = (
  input: unknown,
): ValidationResult<FlightUpdateV1> => validate(flight, input);
export const parseItineraryEvent = (
  input: unknown,
): ValidationResult<ItineraryEventV1> => validate(itinerary, input);
export const parseConnectionAssessment = (
  input: unknown,
): ValidationResult<ConnectionAssessmentV1> => validate(assessment, input);
export const parseIngestionRejection = (
  input: unknown,
): ValidationResult<IngestionRejectionV1> => validate(rejection, input);
export const parseEvent = (input: unknown): ValidationResult<AirlineEventV1> =>
  validate(event, input);
export function parseEventJson(
  input: string,
): ValidationResult<AirlineEventV1> {
  let value: unknown;
  try {
    value = JSON.parse(input);
  } catch {
    return {
      ok: false,
      issues: [issue('', 'INVALID_JSON', 'Payload must be valid JSON.')],
    };
  }
  return parseEvent(value);
}
