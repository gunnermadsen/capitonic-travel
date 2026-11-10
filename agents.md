# Agent Instructions

## Secrets

- The root `.env` file is the canonical source of truth for secret names.
- Use each secret's exact name from `.env` consistently throughout the application, configuration, Docker Compose, and deployment setup.
- Do not rename secret keys or introduce aliases, fallback names, or mappings to differently named environment variables or configuration keys.
- Do not change `.env` secret names to accommodate implementation preferences or work around these requirements.
- Read and pass secrets using their existing names; do not repeatedly translate or remap them between application layers.
- Do not print, log, or include secret values in source code, documentation, or responses. Inspect names without exposing values.

## Ingester Data Products

- Use **data product** for the complete ingestion capability that acquires source data and produces a particular canonical data product for Capatonic Flights. A product includes its orchestration, provider integration, normalization, validation, publication, progress tracking, and operational behavior.
- Name products by the data they produce, not the provider or transport. For example, `flight_updates` is a product; FlightAware and the simulator are sources for that product. A new source for the same canonical output is an adapter, not a new product.
- Use **strategy** for a product's application orchestration, **adapter** for provider-specific acquisition and normalization, and **worker** for execution and lifecycle management. These terms describe distinct responsibilities and are not interchangeable names for the whole product.
- Keep products inside `packages/ingester`. For new products, use `src/products/<product-name>/` with kebab-case directory names, a `<product-name>.module.ts`, and feature-local `application/`, `domain/`, and `adapters/` directories as needed. Use PascalCase classes and role-based filenames such as `<product-name>.strategy.ts`, `<provider>.adapter.ts`, and `<product-name>-worker.service.ts`.
- The existing `src/flight-ingestion/` feature implements the `flight_updates` product. Its relocation to the product layout is a separate implementation change; do not move it merely while documenting this convention.
- Each product must define its canonical contract/version, Kafka topic and key, supported sources/modes, acquisition policy, normalization rules, identity and ordering rules, duplicate handling, progress/recovery semantics, and freshness/error metrics. Keep this definition with the product; reference authoritative contracts and topic manifests rather than duplicating them.
- Canonical schemas and generated types remain owned by `packages/contracts`. Validate untrusted input before use and normalized events before publication. Provider formats must be translated at the adapter boundary; missing data must not be fabricated to pass validation.
- Strategies coordinate the product workflow without depending on provider-specific payloads or connection protocols. Adapters own provider authentication, transport, parsing, and normalization. Workers own start/stop, scheduling or stream consumption, backpressure, and retry coordination. Assign retry ownership explicitly so layers do not multiply retries unexpectedly.
- Define ports to match the actual product's acquisition needs. Polling and streaming adapters must not be forced into a simulator-only sequence or request/response interface. Public ingestion must not require synthetic identifiers.
- Each product owns its logical checkpoint/state namespace and source identity. Preserve event identity across retries, advance durable progress only after acknowledged publication, and define how partial updates become canonical full snapshots. Do not reuse a synthetic cursor for a live source.
- Compose products through explicit NestJS module imports and dependency injection. Keep product-specific providers and state scoped to their owning module. Reuse existing service infrastructure where appropriate without coupling one product to another product's internals.
- Adding a product requires focused tests for normalization, contract validation, identity/replay, failure recovery, and lifecycle behavior relevant to its transport. Adding an adapter must demonstrate compatibility with the existing product contract and recovery rules.
- Do not introduce a plugin framework, generic base-class hierarchy, registry service, extra microservice, or shared package merely to enforce this standard. Extract shared code only when substantial shared behavior or an authoritative source of truth warrants it.
