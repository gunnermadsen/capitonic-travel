export interface AeroApiConfiguration {
  FLIGHTAWARE_API_KEY: string;
  idents: string[];
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
  const idents = (env.AEROAPI_FLIGHTS ?? 'AAL1,DAL1').split(',');
  if (
    idents.length > 4 ||
    new Set(idents).size !== idents.length ||
    idents.some((id) => !/^[A-Z]{3}[0-9]{1,4}[A-Z]?$/.test(id))
  )
    throw new Error(
      'AEROAPI_FLIGHTS must contain one to four ICAO flight designators',
    );
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
    idents,
    intervalMs: integer('AEROAPI_POLL_INTERVAL_MS', 300000, 60000, 3600000),
    output,
    stateDirectory,
  };
}
