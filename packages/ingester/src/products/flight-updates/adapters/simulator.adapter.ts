import type { FlightUpdateV1 } from '@airline/contracts';
import type {
  AcquisitionContext,
  FlightProvider,
} from '../domain/ingestion.ports.js';
import { eventIdentity } from '../domain/event-identity.js';
/** Six deterministic updates per fictional flight pair; source order is revisioned. */
export class SimulatorAdapter implements FlightProvider {
  async acquire(
    context: AcquisitionContext,
    signal: AbortSignal,
  ): Promise<Buffer> {
    signal.throwIfAborted();
    const step = context.sequence % 6;
    return Buffer.from(
      JSON.stringify({
        direction: step % 2 === 0 ? 'inbound' : 'outbound',
        delayMinutes: step === 2 ? 55 : step === 3 ? 60 : 0,
        status:
          step === 4
            ? 'CANCELLED'
            : step === 2 || step === 3
              ? 'DELAYED'
              : 'SCHEDULED',
      }),
    );
  }
  normalize(raw: Buffer, context: AcquisitionContext): FlightUpdateV1 {
    const value: unknown = JSON.parse(raw.toString('utf8'));
    if (
      typeof value !== 'object' ||
      value === null ||
      !('direction' in value) ||
      !('delayMinutes' in value) ||
      !('status' in value) ||
      !['inbound', 'outbound'].includes(String(value.direction)) ||
      typeof value.delayMinutes !== 'number' ||
      !Number.isInteger(value.delayMinutes) ||
      value.delayMinutes < 0 ||
      value.delayMinutes > 1440 ||
      (value.status !== 'SCHEDULED' &&
        value.status !== 'DELAYED' &&
        value.status !== 'CANCELLED')
    )
      throw new Error('Invalid simulator snapshot');
    const inbound = value.direction === 'inbound',
      cycle = Math.floor(context.sequence / 6),
      base = Date.parse(context.startAt) + cycle * 86400000;
    const departure = base + (inbound ? 0 : 7200000),
      arrival = base + (inbound ? 3600000 : 10800000),
      sourceTime = new Date(
        Date.parse(context.startAt) + context.sequence * 1000,
      ).toISOString();
    const originTimezone = inbound ? 'America/Los_Angeles' : 'America/New_York';
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: originTimezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(new Date(departure));
    const localDate = ['year', 'month', 'day']
      .map((kind) => parts.find((part) => part.type === kind)?.value)
      .join('-');
    return {
      event_id: eventIdentity(context.runId, `event:${context.sequence}`),
      schema_version: '1.0.0',
      event_type: 'FLIGHT_UPDATED',
      event_time: sourceTime,
      ingested_at: context.observedAt,
      source: 'simulator',
      operating_mode: 'SYNTHETIC',
      simulation_run_id: context.runId,
      flight_instance_id: eventIdentity(
        context.runId,
        `flight:${cycle}:${value.direction}`,
      ),
      provider_flight_id: null,
      operating_carrier: 'AA',
      flight_number: inbound ? '123' : '456',
      service_date: localDate,
      origin_timezone: originTimezone,
      destination_timezone: 'America/New_York',
      origin: inbound ? 'LAX' : 'JFK',
      destination: inbound ? 'JFK' : 'BOS',
      flight_status: value.status,
      scheduled_gate_arrival_at: new Date(arrival).toISOString(),
      estimated_gate_arrival_at: value.delayMinutes
        ? new Date(arrival + value.delayMinutes * 60000).toISOString()
        : null,
      actual_gate_arrival_at: null,
      scheduled_gate_departure_at: new Date(departure).toISOString(),
      estimated_gate_departure_at: value.delayMinutes
        ? new Date(departure + value.delayMinutes * 60000).toISOString()
        : null,
      boarding_cutoff_at: inbound
        ? null
        : new Date(
            departure + value.delayMinutes * 60000 - 1800000,
          ).toISOString(),
      source_revision: Math.floor((context.sequence % 6) / 2),
      source_updated_at: sourceTime,
      source_timestamp_provenance: 'SOURCE_ISSUED',
      boarding_cutoff_provenance: inbound ? 'UNKNOWN' : 'INFERRED_POLICY',
      boarding_cutoff_policy_version: inbound ? null : 'simulator-cutoff-v1',
    };
  }
}
