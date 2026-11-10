import { Ajv2020 } from 'ajv/dist/2020.js';
import { readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { format } from 'prettier';

// The CommonJS compiler accepts the validated schema object at this boundary.
const generator: unknown = createRequire(import.meta.url)(
  'json-schema-to-typescript',
);
if (
  typeof generator !== 'object' ||
  generator === null ||
  !('compile' in generator) ||
  typeof generator.compile !== 'function'
)
  throw new Error('Missing JSON Schema compiler');
const source = new URL('../src/schemas/events.v1.schema.json', import.meta.url);
const output = new URL('../src/generated/events.v1.ts', import.meta.url);
const schema: unknown = JSON.parse(await readFile(source, 'utf8'));
if (typeof schema !== 'object' || schema === null || Array.isArray(schema))
  throw new Error('Expected JSON Schema object');
const validator = new Ajv2020({ strict: false });
if (!validator.validateSchema(schema)) throw new Error('Invalid JSON Schema');

// allOf contains conditional requirements enforced at runtime, not structural types.
const structural: unknown = JSON.parse(
  JSON.stringify(schema, (key: string, value: unknown) =>
    key === 'allOf' ? undefined : value,
  ),
);
const generated: unknown = await generator.compile(
  structural,
  'AirlineEventV1',
  {
    unknownAny: true,
    ignoreMinAndMaxItems: true,
    bannerComment:
      '/* Generated from events.v1.schema.json. Run npm run generate:contracts. */',
    style: { singleQuote: true, trailingComma: 'all' },
  },
);
if (typeof generated !== 'string')
  throw new Error('Schema compiler returned invalid source');
const expected = await format(generated, {
  parser: 'typescript',
  singleQuote: true,
  trailingComma: 'all',
});
if (process.argv.includes('--write')) {
  await writeFile(output, expected);
  console.log('Contract types generated.');
} else {
  if ((await readFile(output, 'utf8').catch(() => '')) !== expected)
    throw new Error('Stale contract types. Run npm run generate:contracts.');
  console.log('Generated contract types verified.');
}
