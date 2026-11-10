import type { FlightUpdateV1, IngestionRejectionV1 } from '@airline/contracts';
export type Publication = FlightUpdateV1 | IngestionRejectionV1;
export interface AcquisitionContext {
  runId: string;
  startAt: string;
  sequence: number;
  observedAt: string;
}
/** A provider adapter owns acquisition and provider-specific normalization. */
export interface FlightProvider {
  acquire(context: AcquisitionContext, signal: AbortSignal): Promise<Buffer>;
  normalize(raw: Buffer, context: AcquisitionContext): unknown;
}
export interface EventPublisher {
  publish(event: Publication): Promise<void>;
}
export interface IngestionState {
  version: 1;
  runId: string;
  startAt: string;
  nextSequence: number;
  pending: Publication | null;
  lastPublishedAt: string | null;
  lastSourceEventTime: string | null;
}
export interface CheckpointStore {
  load(): Promise<IngestionState>;
  save(state: IngestionState): Promise<void>;
}
