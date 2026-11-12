export interface AeroApiConfiguration {
  FLIGHTAWARE_API_KEY: string;
  airport: string;
  intervalMs: number;
  output: 'kafka' | 'validate';
  stateDirectory: string;
}
export const AEROAPI_CONFIG = Symbol('AEROAPI_CONFIG');
export function aeroApiConfiguration(
  env: NodeJS.ProcessEnv,
  stateDirectory: string,
  output: 'kafka' | 'validate' = 'kafka',
): AeroApiConfiguration {
  if (
    !env.FLIGHTAWARE_API_KEY ||
    !/^[A-Za-z0-9_.@+/=~-]+$/.test(env.FLIGHTAWARE_API_KEY)
  )
    throw new Error('FLIGHTAWARE_API_KEY is required');
  const airport = env.AEROAPI_AIRPORT ?? 'KJFK';
  if (!/^[A-Z]{4}$/.test(airport))
    throw new Error('AEROAPI_AIRPORT must be an ICAO airport code');
  const integer = (
    name: string,
    fallback: number,
    min: number,
    max: number,
  ) => {
    const text = env[name] ?? String(fallback),
      value = Number(text);
    if (
      !/^\d+$/.test(text) ||
      !Number.isSafeInteger(value) ||
      value < min ||
      value > max
    )
      throw new Error(`Invalid ${name}`);
    return value;
  };
  return {
    FLIGHTAWARE_API_KEY: env.FLIGHTAWARE_API_KEY,
    airport,
    intervalMs: integer('AEROAPI_POLL_INTERVAL_MS', 300000, 60000, 3600000),
    output,
    stateDirectory,
  };
}
