# Workspace development

Use Node 22.21.1 and npm 10.9.4 (`.nvmrc`, root engines, and packageManager).
Run dependency installation exclusively from the repository root:

```bash
npm ci
npm run validate
```

`npm install` updates the one root lockfile when introducing dependencies; use
`npm install --workspace @airline/<package> --save-exact <dependency>@<version>`
from the root. Each workspace declares its own dependencies, even when another
workspace or the root already uses them. Do not use legacy peer resolution,
ignore lifecycle scripts, or commit nested installations to work around failures.
The hoisted install strategy is a preference, not a guarantee of a flat tree.
`npm ls --all` checks the installed dependency tree during validation.
Workspace conventions are maintained through standard npm configuration and code review.

`packages/contracts` for authoritative event schemas, generated types, boundary
validation, storage mappings, and compatibility checks. See
[the contracts guide](../packages/contracts/README.md). The ingester is the first application service; Flink processing jobs are not yet deployed. Phase 6 adds TypeScript
`packages/database-migrations` and `packages/kafka-topics` one-shot jobs; see their
package guides for startup and acceptance checks.
Existing infrastructure configuration stays under `common/` with root Compose.

## Package conventions

- Create code packages directly under `packages/<context>` when their feature is
  introduced. Use names `@airline/<context>` and explicit `streamhouse.kind`:
  `library`, `service`, or `job`.
- All TypeScript packages extend `../../tsconfig.base.json`, use `src/` and
  generated `dist/`, and appear in root `tsconfig.json` project references.
  Add project references for internal build dependencies so TypeScript determines
  build order. Runtime dependencies must be declared in `dependencies`.
- Expose deliberate public entry points using explicit `exports` paths to `dist/`.
  Use an empty exports object for executable-only packages. No wildcard exports,
  imports of another workspace's source paths, or imports from another service.
- Organize application services by feature: module, thin controller/transport,
  application orchestration, domain rules, and external adapters. Use NestJS
  explicit dependency injection. Shared logic belongs in shared packages only
  when multiple features actually share that behavior.
- Treat external data as `unknown` and validate it. Strict TypeScript rejects implicit any; Biome rejects explicit any. Type assertions are
  prohibited. Use the public `@airline/contracts` parsers for event boundaries.
- All project-owned Node.js source, including initialization jobs and build
  utilities, uses strict TypeScript. Jobs compile through the root project graph
  and run their `dist/` output. The contract-generation bootstrap
  runs `.ts` directly using the pinned Node runtime and is checked by
  `npm run typecheck:bootstrap`.
- TypeORM migration filenames use `<epochMilliseconds>-PascalCaseDescription.ts`
  with a matching timestamp suffix on the class name. Applied migration source
  files are immutable.

`npm run build` verifies generated contract types and compiles project references; `npm run typecheck` checks the same
strict graph. `npm ls --all` checks the installed dependency tree. `npm run lint` runs Biome. `npm run format` and `format:check` cover workspace-owned source/configs
and this document, leaving unrelated infrastructure formatting untouched.

CI runs a clean root `npm ci` and the same `npm run validate`. Generated output,
private `.local` toolchains/identities, and root dependencies are ignored. Never place project packages under `.local`.

## Validation

CI runs the root installation, build, typecheck, dependency-tree check, lint,
formatting check, and remaining contract, initialization, and ingestion tests.
There is no custom installation guard, workspace-boundary checker, or certificate
lifecycle test suite. A hosted CI run has not been executed from this checkout.

Pinned development tools: TypeScript 5.9.3, Biome 2.6.6, Prettier 3.9.10;
workspace-owned Node declarations 22.19.0. Biome was selected because the ESLint
stack required incompatible transitive majors and nested installations. No
unsafe dependency overrides are used. Platform-specific optional Biome binaries
for other operating systems are intentionally absent on this host; npm reports
those as optional, not invalid dependencies.

## NestJS application conventions

Use feature-oriented [Nest modules](https://docs.nestjs.com/modules) with explicit
imports and deliberately exported providers. `AppModule` composes features;
controllers and transport handlers validate inputs and delegate orchestration.
Domain rules do not depend on transports. Keep external adapters behind small
ports, using explicit `@Inject` tokens and factory providers for infrastructure
clients ([custom providers](https://docs.nestjs.com/fundamentals/custom-providers)).
Avoid circular imports, catch-all global modules, and request-scoped infrastructure
clients. Share code only when multiple consumers need the same behavior.

Use kebab-case filenames with meaningful role suffixes: `.module.ts`,
`.service.ts`, `.strategy.ts`, `.adapter.ts`, and `.test.ts`; use PascalCase class
names. Keep feature-specific `application/`, `domain/`, and `adapters/` directories
inside the owning feature. TypeORM migration naming remains the timestamp and
PascalCase convention above. All authored Node source remains TypeScript.

Validate configuration once in a dedicated module and inject its immutable result.
Use authoritative JSON Schema parsers at untrusted input and publication boundaries;
do not duplicate event interfaces or introduce a competing validation model.
Separate acquisition, normalization, validation, publication, and progress tracking.
Preserve event identity across retries and advance durable progress only after the
broker acknowledgement. Bounded attempts, capped retry backoff, acquisition
timeouts, and sequential processing provide controlled failure and backpressure.

Implement startup and shutdown through [Nest lifecycle hooks](https://docs.nestjs.com/fundamentals/lifecycle-events):
stop acquiring work, drain active work, then close infrastructure clients. Report
liveness separately from readiness, and include event/run identifiers in structured
logs without logging private keys or raw rejected payloads. Test orchestration
through injected ports, plus focused integration checks against real infrastructure.

The ingester uses a standalone Nest application context: dependency injection and
lifecycle hooks without an inbound HTTP or Kafka transport. Its Kafka client module
constructs its authenticated client; its producer module owns publication
and shutdown. A consumer module belongs only in a service that consumes Kafka.
A Postgres module belongs only where database persistence is required, with scoped
credentials and TypeORM migrations rather than runtime schema synchronization.
The ingester persists its single-worker cursors, partial flight state, and pending
publication in a dedicated volume, so it needs neither a consumer nor a database
connection. Synthetic and Firehose state are separate.
See [the ingester guide](../packages/ingester/README.md) for operation and limitations.

Nest common/core are aligned at 11.2.8; no unused HTTP platform or microservice
transport is installed. `tslib` 2.8.1 satisfies the shared dependency ranges and keeps
the installation flat. The ingester and topic-management job each own their small Kafka client setup;
authentication remains separate from topic ACLs. Shared packages are reserved for
authoritative contracts or substantial shared behavior, not trivial client setup.
