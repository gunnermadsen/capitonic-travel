import { connect } from 'node:tls';
import { StringDecoder } from 'node:string_decoder';
import type { IngesterConfiguration } from '../../../configuration/ingester.configuration.js';

/** Incremental JSONL framing with bounded buffering, including split UTF-8 bytes. */
export class FirehoseFrames {
  private readonly decoder = new StringDecoder('utf8');
  private buffer = '';
  push(chunk: Buffer): string[] {
    this.buffer += this.decoder.write(chunk);
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop() ?? '';
    if (
      Buffer.byteLength(this.buffer) > 1048576 ||
      lines.some((line) => Buffer.byteLength(line) > 1048576)
    )
      throw new Error('Firehose frame exceeds limit');
    return lines.filter((line) => line.trim().length > 0);
  }
  finish(): void {
    if ((this.buffer + this.decoder.end()).trim())
      throw new Error('Truncated Firehose frame');
  }
}

export class FirehoseAdapter {
  constructor(private readonly config: Readonly<IngesterConfiguration>) {}
  async *stream(
    pitr: string | null,
    signal: AbortSignal,
  ): AsyncGenerator<string> {
    signal.throwIfAborted();
    const socket = connect({
      host: 'firehose.flightaware.com',
      port: 1501,
      servername: 'firehose.flightaware.com',
      minVersion: 'TLSv1.2',
      rejectUnauthorized: true,
    });
    let closeDeadline: ReturnType<typeof setTimeout> | undefined;
    const stop = () => {
      socket.end();
      closeDeadline = setTimeout(() => socket.destroy(), 1000);
    };
    signal.addEventListener('abort', stop, { once: true });
    const deadline = setTimeout(
      () => socket.destroy(new Error('Firehose connection timeout')),
      10000,
    );
    socket.setTimeout(90000, () =>
      socket.destroy(new Error('Firehose idle timeout')),
    );
    socket.once('secureConnect', () => {
      clearTimeout(deadline);
      const start = pitr === null ? 'live' : `pitr ${pitr}`;
      socket.write(
        `${start} username ${this.config.FIREHOSE_USERNAME} password ${this.config.FIREHOSE_API_KEY} events "flifo departure arrival cancellation" airport_filter "${this.config.firehoseAirports}" optype_filter airline keepalive 15\n`,
      );
    });
    const frames = new FirehoseFrames();
    try {
      // Async iteration pauses socket reads while the caller persists/publishes each item.
      for await (const chunk of socket) {
        if (!Buffer.isBuffer(chunk))
          throw new Error('Unexpected socket encoding');
        for (const line of frames.push(chunk)) {
          signal.throwIfAborted();
          yield line;
        }
      }
      if (!signal.aborted) {
        frames.finish();
        throw new Error('Firehose disconnected');
      }
    } finally {
      clearTimeout(deadline);
      if (closeDeadline) clearTimeout(closeDeadline);
      signal.removeEventListener('abort', stop);
      socket.destroy();
    }
  }
}
