# Agent Instructions

## Delivering changes

- Commit and push completed repository changes unless the user explicitly asks to keep them local. Report the remote branch and commit so other computers can retrieve the changes.

## Secrets

- The root `.env` file is the canonical source of truth for secret names.
- Use each secret's exact name from `.env` consistently throughout the application, configuration, Docker Compose, and deployment setup.
- Do not rename secret keys or introduce aliases, fallback names, or mappings to differently named environment variables or configuration keys.
- Do not change `.env` secret names to accommodate implementation preferences or work around these requirements.
- Read and pass secrets using their existing names; do not repeatedly translate or remap them between application layers.
- Do not print, log, or include secret values in source code, documentation, or responses. Inspect names without exposing values.

## Ingester Data Products

- Use **data product** for the complete ingestion capability that acquires source data and produces a particular canonical data product for Capatonic Flights. A product includes its orchestration, provider integration, normalization, validation, publication, progress tracking, and operational behavior.
- Name products by the data they produce, not the provider or transport. For example, `flight_updates` is a product; FlightAware and the simulator are sources for that product. Add another source for the same canonical output within the existing product service.
- Register products in one `ProductsModule`. Put each product's acquisition, normalization, recovery, and lifecycle in one `<product-name>.service.ts` file with one product service class. Keep source-specific methods and small pure helpers in that file; do not add separate workers, strategies, adapters, ports, or per-product modules.
- Keep shared Kafka publishing, configuration, health reporting, and canonical contracts outside product services. Keep product tests separate.
- Each product must define its canonical contract/version, Kafka topic and key, supported sources/modes, acquisition policy, normalization rules, identity and ordering rules, duplicate handling, progress/recovery semantics, and freshness/error metrics. Keep this definition with the product; reference authoritative contracts and topic manifests rather than duplicating them.
- Canonical schemas and generated types remain owned by `packages/contracts`. Validate untrusted input before use and normalized events before publication. Provider formats must be translated when acquired; missing data must not be fabricated to pass validation.
- Each product owns its logical checkpoint/state namespace and source identity. Preserve event identity across retries, advance durable progress only after acknowledged publication, and define how partial updates become canonical full snapshots. Do not reuse a synthetic cursor for a live source.
- Construct product services in `ProductsModule` using shared infrastructure providers. Select only one configured source lifecycle per product instance.
- Adding a product requires focused tests for normalization, contract validation, identity/replay, failure recovery, and lifecycle behavior relevant to its transport. Adding a source must demonstrate compatibility with the existing product contract and recovery rules.
- Do not introduce a plugin framework, generic base-class hierarchy, registry service, extra microservice, or shared package merely to enforce this standard. Extract shared code only when substantial shared behavior or an authoritative source of truth warrants it.
