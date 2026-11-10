import { createHash } from 'node:crypto';
export function eventIdentity(runId: string, key: string): string {
  const hex = createHash('sha256').update(`${runId}:${key}`).digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
