import type { AeroApiConfiguration } from '../aeroapi.configuration.js';
import {
  parseAeroApiResponse,
  type AeroApiSchedule,
} from '../domain/aeroapi.contract.js';
export class AeroApiHttpError extends Error {
  constructor(
    readonly status: number,
    readonly retryAfterMs: number,
  ) {
    super(`AeroAPI HTTP ${status}`);
    this.name = 'AeroApiHttpError';
  }
}
export interface AeroApiSource {
  acquire(
    schedule: AeroApiSchedule,
    now: Date,
    signal: AbortSignal,
  ): Promise<unknown[]>;
}
export class AeroApiAdapter implements AeroApiSource {
  constructor(
    private readonly config: AeroApiConfiguration,
    private readonly request: typeof fetch = fetch,
  ) {}
  async acquire(
    schedule: AeroApiSchedule,
    now: Date,
    signal: AbortSignal,
  ): Promise<unknown[]> {
    const url = new URL(
      `https://aeroapi.flightaware.com/aeroapi/airports/${this.config.airport}/flights/${schedule}`,
    );
    url.searchParams.set('type', 'Airline');
    url.searchParams.set('max_pages', '1');
    url.searchParams.set(
      'start',
      new Date(now.getTime() - 2 * 3600000)
        .toISOString()
        .replace(/\.\d{3}Z$/, 'Z'),
    );
    url.searchParams.set(
      'end',
      new Date(now.getTime() + 6 * 3600000)
        .toISOString()
        .replace(/\.\d{3}Z$/, 'Z'),
    );
    const response = await this.request(url, {
      headers: {
        'x-apikey': this.config.FLIGHTAWARE_API_KEY,
        accept: 'application/json',
      },
      redirect: 'error',
      signal: AbortSignal.any([signal, AbortSignal.timeout(15000)]),
    });
    if (!response.ok) {
      const retry = response.headers.get('retry-after');
      const wait =
        retry === null
          ? 0
          : /^\d+$/.test(retry)
            ? Number(retry) * 1000
            : Math.max(0, Date.parse(retry) - Date.now());
      await response.body?.cancel();
      throw new AeroApiHttpError(
        response.status,
        Number.isFinite(wait) ? wait : 0,
      );
    }
    const reader = response.body?.getReader();
    if (!reader) throw new Error('Empty AeroAPI response');
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        length += chunk.value.byteLength;
        if (length > 1048576) throw new Error('AeroAPI response exceeds limit');
        chunks.push(chunk.value);
      }
    } finally {
      await reader.cancel();
    }
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return parseAeroApiResponse(value, schedule);
  }
}
