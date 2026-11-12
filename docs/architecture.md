# Local Airline Streamhouse — Architecture

Baseline decision date: 2026-11-08. Phase 0 was accepted by the project owner.
Current foundation operations are in [local-development.md](local-development.md);
measured gate results are in [infrastructure-verification.md](infrastructure-verification.md).

## Status and evidence

The system specification is accepted. Kafka, Flink, and RustFS foundation gates
were verified on 2026-11-08; current database/catalog and upgrade evidence is in
[infrastructure-verification.md](infrastructure-verification.md). Docker access
requires the execution environment's approved socket access. No application
packages or custom initialization jobs are introduced before foundation acceptance.

Workspace acceptance: root npm workspace/tooling gates were verified on
2026-11-08. See [workspace-development.md](workspace-development.md) for commands,
package conventions, CI configuration, and acceptance evidence. Only the workspace
tooling package is introduced; business packages remain deferred.

Identity lifecycle acceptance: manifest-driven generation, inspection, isolated
rotation/CA-overlap tests, and live rejection checks passed on 2026-11-08 while
preserving existing CA and leaf material. See [identity-operations.md](identity-operations.md).
No business services or later-phase initialization jobs were introduced.

## 1. Component and version baseline

All containers, including build stages and tools, MUST run natively on
`linux/arm64`. No AMD64 images, QEMU, or Rosetta fallback. Multi-platform image
indexes are acceptable only with an explicitly selected ARM64 child manifest.

| Component | Selected version / artifact | Purpose and constraints |
| --- | --- | --- |
| Kafka | `apache/kafka:4.4.0` JVM image | One combined KRaft broker/controller; no ZooKeeper; TLS-only application listeners |
| Node Kafka client | `kafkajs@2.2.4` | Pure JavaScript; mTLS; explicit protocol/transaction integration tests required later |
| Flink | `apache/flink:2.2.1-java17` | Identical derived image for JobManager, TaskManager, SQL client |
| Flink Java | Java 17 supplied by pinned Flink image | JDK TLS provider; image-pinned JDK, exact vendor/build captured at Phase 1 build |
| Kafka SQL connector | `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2` | Matches Flink 2.2; do not add a second conflicting Kafka client JAR |
| Iceberg | `org.apache.iceberg:iceberg-flink-runtime-2.2:1.12.0` | Flink 2.2 runtime bundle; Iceberg format version 2 initially |
| Iceberg Hadoop client classes | `hadoop-client-api:3.4.2` and `hadoop-client-runtime:3.4.2` | Required by the Iceberg Flink catalog factory; SHA-256 pinned in the runtime image; separate from Flink’s isolated S3 plugin |
| Iceberg S3 IO | `org.apache.iceberg:iceberg-aws-bundle:1.12.0` | Same Iceberg version; S3FileIO for table objects |
| Flink checkpoint S3 plugin | `flink-s3-fs-hadoop:2.2.1` | Isolated plugin directory; distinct from Iceberg S3FileIO |
| RustFS | `rustfs/rustfs:1.0.1` | Single-node local S3 service, TLS and signed authenticated requests |
| Polaris | `apache/polaris:1.8.0` | Iceberg REST catalog with relational JDBC PostgreSQL persistence |
| PostgreSQL | 17.11; build base `arm64v8/postgres:17.11-alpine` | Separate application and catalog databases in one local server |
| TimescaleDB | 2.29.1, native source build | Build extension against PG17 headers on native Alpine ARM64 with GCC/CMake and matching musl; no copied glibc or AMD64 binaries |
| Node.js | 22.21.1 | ARM64 host/container runtime; `node:22.21.1-bookworm-slim`, explicitly selected with linux/arm64 |
| npm | 10.9.4 | One root install and lockfile; independently pin version, do not assume bundled version |
| NestJS | 11.2.8 | Aligned core/common; standalone ingester, no HTTP platform dependency |
| TypeScript | 5.9.3 | Strict TypeScript for all project-owned Node.js code, including initialization jobs |

These deliberately fixed versions are not advertised as latest or as security
certified. Before building, check upstream advisories, record immutable ARM64
image digests and upstream artifact SHA-512/SHA-256 checksums. A material issue
requires updating this matrix and rechecking the compatibility edges together.

### Streaming compatibility revision — 2026-11-08

Kafka 4.4.0 runs in KRaft mode, retaining the existing cluster ID and data volume.
Flink 2.2.1 is the newest stable release supported by the published Kafka SQL
connector 5.0.0-2.2. Flink 2.3.0 is not selected because that connector's published
support matrix stops at 2.2. Iceberg 1.12.0 supports Flink 2.2; its runtime and AWS
bundle are aligned. Java 17 and the matching Flink 2.2.1 S3 plugin are retained.
The future Node client baseline is not certified against Kafka 4.4 by these Java
connector checks; its protocol compatibility remains a prerequisite to application
implementation, and must not be inferred from the earlier Kafka 3.9 selection.

Sources: [Flink connector matrix](https://flink.apache.org/downloads/),
[Iceberg 1.12.0](https://iceberg.apache.org/blog/apache-iceberg-1.12.0-release/),
[Kafka upgrade instructions](https://kafka.apache.org/44/getting-started/upgrade/).

### Database/catalog security revision — 2026-11-08

The Phase 2 advisory check identified material fixes missing from the original
pins. PostgreSQL is revised to 17.11 (same major), TimescaleDB to 2.29.1, and
Polaris/server-admin to 1.8.0. These supersede the original baseline selections
below; their ARM64 digests and source checksums are locked in executable
infrastructure. The vendor's corresponding schema lifecycle is authoritative.
The revised combination must pass the database/catalog gates before acceptance.

Sources: [PostgreSQL 17 security](https://www.postgresql.org/support/security/17/),
[TimescaleDB permission-check fix](https://github.com/timescale/timescaledb/security/advisories/GHSA-hcfx-29v5-2rcw),
[Polaris advisories](https://polaris.apache.org/community/security-report/).

### ARM64 evidence and remaining checks

Current ARM64 child manifests are locked in `docker-compose.local.yml` and the Dockerfiles:

| Image | ARM64 digest |
| --- | --- |
| apache/kafka:4.4.0 | `sha256:77b56ddfb7104fc5814a3d0127fcff49983220c7c7ad9bfc1f0af4d439228329` |
| apache/flink:2.2.1-java17 | `sha256:87c436d143ddeafbe977dd299ee63cef574b57688dc29f13cad90e0c7e2f6126` |
| rustfs/rustfs:1.0.1 | `sha256:3bc0a69f7636faf49cbddd38901bb688dc7fe3fa6d2e2f87cc56e71890f66bc8` |
| arm64v8/postgres:17.11-alpine | `sha256:1108a8fe22dff4eab48870e081be046d549ddc1b6f5972e0a766eaf1e0b1003c` |
| apache/polaris:1.8.0 | `sha256:74e6416c75f24563fc62f4ed52699d4fb872741b6a3e0af77fdc551b4ee72fce` |
| apache/polaris-admin-tool:1.8.0 | `sha256:9f85ebe1fdfa9eb1214c4d5d343de29f76fef3dda7b4eb11c6767acc7e6c4fa4` |

Node's ARM64 manifest and application dependencies remain later-phase checks.
Java artifacts are bytecode; native extension loading and client paths must also
be verified. No AMD64 fallback is permitted. Runtime TLS uses the JDK provider.

### Compatibility acceptance boundaries

- Phase 0: select exact releases, native ARM64 routes, configuration boundaries,
  engine/peer constraints, and acceptance criteria. Complete at specification level.
- Artifact acquisition in the owning phase: obtain actual image manifests and
  artifact checksums, record content locks, and reject non-ARM64 native artifacts.
- Phase 1: verify Kafka mTLS/ACLs, Flink secure registration, S3 reads/writes,
  connector loading, and persistent infrastructure. No application implementation.
- Phase 2: verify PG17 extension load, certificate-only database clients,
  Polaris JDBC persistence, REST authentication, and Flink Iceberg write/read.
- Phase 3: actual root dependency-tree validation. Do not claim it passed now.

## 2. Topology, DNS, ports, and networks

Use Docker Compose for the local deployment. Production deployment is out of
scope. Stable service DNS names are certificate SANs. Bind host publications to
127.0.0.1 only. Never advertise localhost to another container.

| Service | Container endpoints | Host endpoint | Network membership |
| --- | --- | --- | --- |
| kafka | 9092 INTERNAL, 9093 CONTROLLER, 9094 HOST | 127.0.0.1:19092 -> 9094 | streaming |
| flink-jobmanager | 8081 HTTPS REST; 6123 RPC; 6124 blob | 127.0.0.1:18081 -> 8081 | streaming, lake |
| flink-taskmanager | 6121 data; 6122 RPC | none | streaming, lake |
| flink-sql-client | ephemeral CLI, no listener | none | streaming, lake |
| rustfs | 9000 HTTPS S3; 9001 HTTPS console | 127.0.0.1:19000 / :19001 | lake |
| postgres | 5432 TLS PostgreSQL | 127.0.0.1:15432 -> 5432 | database, database-host |
| polaris | 8443 HTTPS REST | 127.0.0.1:18181 -> 8443 | lake, database |

Names of project-scoped bridge networks: `streaming`, `lake`, `database`.
Database network is internal. Other networks are not internet-denial boundaries;
future public ingestion needs controlled outbound HTTPS. Network membership
limits exposure; authorization remains mandatory at services.

Kafka advertised listeners: INTERNAL=`kafka:9092`, HOST=`localhost:19092`.
Controller quorum is node 1 at `kafka:9093`; controller is not advertised as a
client listener. All listener protocols are SSL. Internal listener is also the
inter-broker listener. Cluster ID is generated once and retained with data.
Single-node internal topic replication factors and transaction-state minimum ISR
are 1. This provides persistence, not high availability.

No public application endpoints or application networking are implemented now.
Reserve host ports 13000/13001 for the later API/itinerary service.

## 3. Persistent storage and ownership

Use Compose-managed named volumes scoped by project name; never a common global
volume shared by development and tests. Logical volume names:

| Volume | Mount / content | Owning process |
| --- | --- | --- |
| kafka-data | `/var/lib/kafka/data`, log and KRaft metadata directories | Kafka image appuser |
| rustfs-data | `/data`, all object data and associated service metadata | RustFS runtime user |
| postgres-data | `/var/lib/postgresql/data`, both databases | postgres user |
| flink-jm-work | `/opt/flink/work`, JobManager working files | flink user |
| flink-tm-work | `/opt/flink/work`, TaskManager working files | flink user |

Ownership is finalized by policy: each volume is owned by the non-root runtime
account listed above, with no cross-service sharing of writable directories.
At image acquisition, resolve that account's numeric UID/GID from the selected
image's /etc/passwd (including entrypoint privilege changes), record it in the
phase verification record, and use it for initialization. A numeric ID is an
image-build fact, not a portable constant. Never use world-writable permissions.
Bootstrap can initialize ownership before dropping to the runtime account; no
application runs with bootstrap credentials. Flink working volumes
are not substitutes for durable checkpoints. Polaris catalog metadata resides
in PostgreSQL, not an in-memory metastore or an ephemeral container directory.

RustFS buckets: `airline-dev-checkpoints`, `airline-dev-savepoints`,
`airline-dev-warehouse`. Prefixes include job ID/generation for state and catalog
namespace/table IDs for warehouse data. Use HTTPS endpoint `https://rustfs:9000`,
path-style addressing, region `us-east-1` as a local signing convention (not an
AWS deployment). Checkpoints are retained on cancellation; savepoints require
explicit deletion. Never apply blind age-based deletion to live Iceberg objects.

Secrets and local identity material: future ignored `.local/identity/` and
`.local/secrets/` directories, separate per environment, outside tracked source.
CA private keys are not mounted into services. Each container gets read-only
mounts of only its own leaf keys and required public trust material. Host owner
is the developer, directories 0700, private keys 0600 with container-specific
ownership as needed. Use secret files rather than command-line credentials.
No secrets or directories containing secrets have been created in Phase 0.

## 4. Identities and permissions

Use separate local issuing CAs for Kafka, Flink internal, Flink REST, PostgreSQL,
and infrastructure HTTPS. This limits trust scope. Kafka clients in Flink use
Kafka-issued identities, not Flink internal identities. Certificates use RSA
3072/SHA-256, leaf validity 90 days, CA validity 3 years. Server leaves have exact
DNS SANs, localhost and 127.0.0.1 only when host access is approved. Client leaves
have clientAuth EKU; runtime peers needing both directions have both EKUs.
Do not disable endpoint identification. Keep existing CA/key material intact.

| Identity | Authentication and permitted use |
| --- | --- |
| kafka-broker-1 / kafka-controller-1 | Distinct listener identities; Kafka CA, both EKUs; bootstrap superusers limited to infrastructure |
| kafka-bootstrap-admin | Operator-only Kafka administrator; not mounted into applications |
| kafka-topic-manager | Later job; manage declared topics/configs/ACLs; ACL administration necessarily has cluster-level privilege |
| ingester | Write flight.updates and ingestion.rejections only |
| itinerary-service | Write itinerary.events only |
| flink-assessor | Read flight.updates/itinerary.events with named group; write connection.assessments with own transactional-ID prefix |
| flink-archiver | Read archival input topics with named groups; separate Kafka identity |
| connection-api | Read connection.assessments with its own group; no Kafka writes initially |
| flink-jobmanager / flink-taskmanager | Distinct internal certificates, restricted internal trust/pinning; no application access to internal keys |
| flink-operator / flink-sql-client | Flink REST client certificates; treat accepted REST clients as privileged operators |
| postgres-server | ServerAuth, SAN postgres/localhost/127.0.0.1 |
| database-migration | Mapped to airline_migrator; DDL only in application database/schema |
| connection-api-db | Mapped to airline_runtime; required SELECT/INSERT/UPDATE only |
| polaris-db | Mapped to polaris_runtime in catalog database only |
| postgres-bootstrap-admin | Infrastructure-only role bootstrap and extension administration |
| polaris-bootstrap-admin | Operator-only catalog/principal bootstrap |
| polaris-flink | OAuth client credentials; authorized catalog/namespace access |
| rustfs-flink-state | S3 key scoped to checkpoint/savepoint buckets |
| rustfs-flink-archive | S3 key scoped to warehouse bucket |
| rustfs-polaris | S3 key scoped to warehouse operations needed by catalog |

Kafka StandardAuthorizer: deny when no ACL matches. Use exact principals with
documented CN-to-principal mapping. Topic/group/transactional-ID resources receive
minimal ACLs; idempotent producers receive required idempotent-write privileges.
Topic manager's cluster ACL privileges cannot be represented as a topic-only
security boundary; compensate with isolated one-shot execution and restricted keys.

Flink: enable internal SSL and REST SSL with REST client authentication; JDK TLS,
hostname verification enabled. Internal trust must accept only runtime identities
(pin allowed peer certificates or use an exclusively scoped CA as supported).
No claim of fine-grained REST user authorization. TLS 1.2 is the initial Java
compatibility baseline; never fall back to plaintext.

PostgreSQL: separate databases `airline` and `polaris`; application schema
`airline`, catalog-owned schema governed by Polaris bootstrap. Remote rules use
hostssl + cert with explicit pg_ident mappings; reject hostnossl and unmatched
clients. Clients use verify-full (or equivalent CA + hostname verification).
Migration role owns application schema; runtime has no ownership/DDL rights.
TimescaleDB extension is enabled only by infrastructure bootstrap. The future
connection_current table is regular relational storage, not a hypertable.

Polaris: relational-jdbc backend, its own bootstrap/schema lifecycle. TLS on
catalog/OAuth endpoints; client-secret authentication plus catalog authorization.
RustFS: native HTTPS plus S3 signed requests; no anonymous buckets. Do not assume
RustFS supports AWS STS credential vending: initially configure distinct static,
scoped S3 credentials in Polaris and Flink and disable reliance on vending.
Polaris 1.8.0 explicitly supports non-STS S3 storage through
`stsUnavailable: true` in storage configuration. Use endpoint
`https://rustfs:9000`, path-style addressing, allowed location
`s3://airline-dev-warehouse/`, and region `us-east-1`. Catalog operations use a
Polaris-specific warehouse credential; Flink uses its own warehouse credential.
Do not request vended credentials from this catalog.

Release-specific persistence selection: `POLARIS_PERSISTENCE_TYPE=relational-jdbc`,
`QUARKUS_DATASOURCE_USERNAME=polaris_runtime`, and
`QUARKUS_DATASOURCE_JDBC_URL` pointing to database polaris. Use the 1.8.0 admin bootstrap tooling and its schema v6 lifecycle; operator
upgrade details are recorded in database-catalog-operations.md.
Do not use a current unreleased schema or EclipseLink. The bootstrap owner
creates the catalog tables; the runtime role receives the required DML/sequence
privileges without owning the database. Keep catalog migration separate.

The JDBC URL carries `sslmode=verify-full`, `sslrootcert` pointing to the CA,
`sslkey` pointing to a PKCS12 client bundle, and `currentSchema=polaris_schema`
(unquoted POLARIS_SCHEMA resolves to this lowercase name). Use PKCS12 alias
`user`, .p12 extension, and include the client chain. PostgreSQL's cert rule
maps the certificate to polaris_runtime; no password fallback. pgJDBC supports
this route independently of Polaris's username/password documentation example.
TLS handshake and least-privilege correctness are Phase 2 tests. Configure
Quarkus HTTPS at 8443 and disable its plaintext HTTP listener. Java truststores
must include the private HTTPS CA for both catalog and RustFS clients.

## 5. Development and disposable data

Persistent project: `airline-dev`. Disposable verification projects:
`airline-test-<run-id>`. Separate volumes, credentials, CAs, catalog realms,
buckets, and ports. Test tools may delete only the named test project's resources.
Normal shutdown/recreation preserves volumes. Volume removal, bucket purge,
catalog reset, and CA replacement are explicit destructive operations.

Phase 1 vendor-tool checks may create a disposable `smoke_<run-id>` topic and
test objects. They are not application topic initialization. Later topic manifests
remain authoritative. No production Capitonic data or existing drain resources
belong in this project. AWS targets are outside this local experiment.

## 6. Resource and capacity budgets

The initial profile fits the confirmed Docker allocation: 4 CPUs, 7.503 GiB RAM.
No resource allocation changes are made. Use a small synthetic dataset, one
TaskManager slot, default job parallelism 1, and one combined assessment/archive
job initially. Separate production-style scaling is outside this learning profile.

| Process | CPU ceiling | Container RAM ceiling | Initial process settings |
| --- | --- | --- | --- |
| Kafka | 0.75 | 1280 MiB | JVM heap 512 MiB |
| Flink JobManager | 0.5 | 1024 MiB | process memory 768 MiB |
| Flink TaskManager | 1.25 | 1792 MiB | process memory 1536 MiB; 1 slot |
| RustFS | 0.5 | 768 MiB | low-volume single-node profile |
| PostgreSQL (Phase 2) | 0.5 | 512 MiB | shared_buffers 128 MiB; max_connections 30 |
| Polaris (Phase 2) | 0.5 | 768 MiB | JVM heap 384 MiB; JDBC max pool 5 |

Phase 1 ceilings total 4864 MiB (4.75 GiB); Phase 2 totals 6144 MiB (6 GiB).
The ephemeral SQL client is capped at 512 MiB, leaving about 1 GiB for the Docker
VM and overhead. These are initial engineering budgets, not measured guarantees.
If a component's minimum or observed use exceeds its allocation, fail that
phase's startup gate and revise the budget rather than silently overcommit.
Build images sequentially before bringing up services; stop ephemeral tools
when finished. Later application workloads require their own budget review.

Storage working budget: Kafka 8 GiB, RustFS 20 GiB, PostgreSQL 4 GiB, Flink
working data 4 GiB, images/build cache 16 GiB, headroom 8 GiB. Named volumes do
not enforce these quotas: monitor usage. Pause ingestion at 80% of its budget;
keep at least 20 GiB host free space. Synthetic baseline 1 event/sec, maximum
2 events/sec aggregate across retained business topics, <=4 KiB/event until
capacity is measured. This limit includes generated assessments and rejections.

History-topic retention initially 7 days, outage/recovery objective <=24 hours.
Three partitions per business topic, replication factor 1, min ISR 1. Initial
retention.ms=604800000 and retention.bytes=536870912 per partition: 12 business
partitions give a nominal 6 GiB cap, reserving 2 GiB for active segments, internal
topics, and overhead. Segment size target 64 MiB. A byte cap can expire events
before seven days; track the oldest available offset/time and never advertise
an unconditional seven-day guarantee. At 2 x 4 KiB events/sec, seven days is
about 4.62 GiB before overhead; skew can still hit an individual partition cap.
Retention and the <=24-hour recovery objective must be demonstrated under the
measured workload in later acceptance tests.
Use delete cleanup for history; no compaction that removes input history.

## 7. Operating modes

Default `synthetic`: seeded fixtures, deterministic clock/scenario control,
simulation_run_id required, stable IDs across retries, no provider credentials.
`public`: opt-in provider adapter selected in Phase 10, fictional itineraries,
record provider and timestamp provenance, obey storage/use restrictions.
One authoritative provider per flight instance per mode. Both modes share the
same canonical contracts and business rules. Keep mode datasets in separate
environment instances/catalog namespaces; switching mode must not reuse synthetic
state as live state. Missing timing yields UNKNOWN, never fabricated certainty.

## 8. Naming and workspace standards

- Compose services/package directories: lowercase kebab-case.
- Topics: flight.updates, itinerary.events, connection.assessments,
  ingestion.rejections. Each environment has its own Kafka instance.
- JSON fields, SQL columns/tables: snake_case. JSON Schemas use product name and
  integer major schema version; additive evolution rules are defined in Phase 5.
- Event types use explicit product/action names; itinerary payload actions remain
  CREATED, UPDATED, CANCELLED. Do not ambiguously duplicate envelope event_type.
- Flight topic key: flight_instance_id. Itinerary key: connection_id (events
  for multiple connections are emitted separately). Assessment key: connection_id.
- Consumer groups: `<service>.<purpose>.v1`; transactional IDs:
  `<environment>.<job>.<generation>.<subtask>` with stable recovery semantics.
- Catalog: airline; Iceberg namespaces: synthetic and public; table names match
  products. Per-test catalogs/namespaces carry unique run IDs.
- Identity CNs: `<environment>.<service>.<purpose>`; server SANs match service DNS.
- Microservices, shared libraries, and planned initialization jobs live directly under packages/*; root package.json,
  package-lock.json and node_modules only. Every workspace declares dependencies.
  Keep installations at the root; CI uses npm ls --all to check dependency compatibility.
- Flink SQL/Java job sources live under packages/flink-jobs; JVM build tooling
  stays inside that package. NestJS rules apply to application microservices,
  not to JVM Flink job code. No packages or jobs are created in Phase 0.

## Compatibility decisions

- Node 22.21.1 satisfies Nest core 11.2.8's declared Node >=20 requirement.
  Use core/common 11.2.8 together, reflect-metadata 0.2.2,
  and rxjs 7.8.2 (compatible with core/common's published peer ranges).
  Do not install optional Nest transports/websockets or class-validator merely
  to satisfy optional peers. JSON Schema remains the authoritative validation
  route. TypeScript 5.9.3 compiles applications; initialization jobs compile TypeScript to Node ESM.
- npm 10.9.4 is an explicit independent toolchain pin. Root-only installation,
  strict peer checking, and recursive node_modules-layout validation are Phase 3
  gates. No package install or complete dependency-tree claim is made in Phase 0.
- Kafka 4.4.0 uses KRaft. Flink Kafka connector 5.0.0-2.2 is aligned with
  Flink 2.2.1 / Java 17. Actual mTLS produce/consume is a foundation gate;
  future Node clients, transactions, and read_committed semantics are tested in
  their owning application phases. KafkaJS's former baseline is not certified
  for the upgraded broker by this Java connector test.
- Iceberg 1.12.0 publishes a Flink 2.2 runtime. Format v2 and the REST catalog
  protocol are the interoperability boundary; Polaris and the engine need not
  bundle identical Iceberg library versions.
- TimescaleDB 2.29.1 is compiled natively against PostgreSQL 17.11 headers and
  the matching Alpine ABI. ICU development headers are required by this build.
  No Toolkit or hypertables are required for the initial application model.
- Flink state storage and Iceberg FileIO use separate credential configuration
  and separate classloading/plugin locations. Both must trust the RustFS TLS CA.
- Initial compression uses JavaScript/JDK-supported defaults (Kafka producer
  gzip or none); do not introduce native codecs until ARM64 compatibility is
  verified. No native RocksDB or OpenSSL dependency is required initially.

## Phase 0 acceptance and implementation handoff

| Original Phase 0 task | Final specification outcome |
| --- | --- |
| Component inventory | Complete: six required component families plus SQL client role |
| Compatible pinned version matrix | Complete: exact release selections and native build routes; runtime tests explicitly assigned |
| DNS, ports, networks, volumes, ownership | Complete: endpoint table, persistent stores, runtime-account ownership policy |
| Certificate identities and permissions | Complete: trust domains, individual principals, ACL/database/catalog boundaries |
| Disposable vs persistent development data | Complete: isolated project/data/identity namespaces and deletion boundaries |
| CPU, memory, storage budgets | Complete: profile based on user-confirmed 4 CPU / 7.503 GiB Docker allocation |
| Synthetic/public modes | Complete: default synthetic, opt-in public, one source per flight instance |
| Naming conventions | Complete: services, topics, schemas, tables, groups and identities |
| No application packages or custom initialization code | Satisfied: documentation only |

Phase 0 gate: COMPLETE at specification level. The following mandatory execution
checks remain in their owning phases; none has been represented as passed:

| Owner | Required check / deliverable |
| --- | --- |
| Phase 1 preflight | Docker engine access; registry access; no conflicting host ports; actual disk capacity |
| Phase 1 image build | Resolve Kafka/Flink/RustFS ARM64 manifests; lock immutable digests; obtain connector JAR checksums from Maven Central/Apache release artifacts; verify bytes and native dependencies; capture Java build and runtime UIDs/GIDs |
| Phase 1 gate | Secure Kafka/Flink/RustFS startup, authenticated vendor-tool checks, container-recreation persistence |
| Phase 2 acquisition/build | Lock Polaris/Postgres ARM64 digests and TimescaleDB source checksum; native source compilation; derive catalog bootstrap tool from the same Polaris release/runtime if no verified ARM64 tool image exists |
| Phase 2 gate | Certificate-only PostgreSQL/JDBC connectivity, catalog persistence, Iceberg create/write/read/drop smoke test |
| Phase 3 gate | Resolve Node ARM64 child manifest, pin npm, install from root, validate actual peer resolution and absence of nested node_modules |

Artifact-lock procedure: resolve the selected release's index and ARM64 child,
verify its config architecture is arm64, record both digests and provenance,
and reference the immutable digest with platform linux/arm64 in executable
configuration. Download JARs/source from the named upstream release, verify the
published checksum/signature where provided, and record the verified hash in the
owning phase's lock record. If no published checksum exists, record the exact
upstream source revision and locally computed hash; do not present that as
independent signature verification. Missing artifacts, architecture mismatch,
failed checksum, incompatible release metadata, or material security findings
stop that phase and require a documented matrix revision. Never use latest tags,
AMD64 substitution, disabled TLS verification, or silent dependency downgrades.

Implementation is ready to start once its environment preflight is available.
The known sandbox socket/DNS restrictions are execution blockers, not missing
architecture decisions. No Compose, Dockerfile, certificate, package manifest,
application, initialization job, or container was created by this phase.

## Source references

Upstream/registry sources consulted on the decision date:

- Kafka distribution: https://kafka.apache.org/39/getting-started/docker/
- Kafka ARM64: https://hub.docker.com/layers/apache/kafka/3.9.1/images/sha256-39bc3b30084ad6ab33ad2c9a525f15c942bf097b18cb2a1825afa6df3411f8b5
- KafkaJS versions: https://kafka.js.org/versions
- Flink releases/connectors: https://flink.apache.org/downloads/
- Flink ARM64: https://hub.docker.com/layers/apache/flink/1.20.3-scala_2.12-java17/images/sha256-a2f183ef05bfbde401cd9ae5e3320e7ae06edc1fb8a9c00f424bc84ea9dd2b72
- Flink TLS: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/security/security-ssl/
- Iceberg version support: https://iceberg.apache.org/multi-engine-support/
- Iceberg releases: https://iceberg.apache.org/releases/
- RustFS ARM64: https://hub.docker.com/layers/rustfs/rustfs/1.0.1/images/sha256-3bc0a69f7636faf49cbddd38901bb688dc7fe3fa6d2e2f87cc56e71890f66bc8
- RustFS TLS: https://docs.rustfs.com/en/integration/tls-configured
- Polaris distribution: https://hub.docker.com/r/apache/polaris/tags
- Polaris releases: https://polaris.apache.org/downloads/
- Polaris 1.2 persistence: https://polaris.apache.org/releases/1.2.0/metastores/
- Timescale image source: https://github.com/timescale/timescaledb-docker
- Node release: https://nodejs.org/en/blog/release/v22.21.1
- TypeScript releases: https://github.com/microsoft/TypeScript/releases

Additional evidence used to finalize the specification:

- Polaris 1.2 release notes (stsUnavailable): https://apache.googlesource.com/polaris/+/HEAD/CHANGELOG.md (section 1.2.0-incubating)
- Polaris JDBC schema-version mapping: https://polaris.apache.org/in-dev/unreleased/metastores/relational-jdbc/ (historical version table only)
- pgJDBC certificate configuration: https://jdbc.postgresql.org/documentation/ssl/
- PostgreSQL ARM64 base: https://hub.docker.com/layers/arm64v8/postgres/17.6-alpine/images/sha256-9b9fb55f7e3b2149854def33c728b781dc44d1c5e86492ad62912a527ae234b3
- Node exact-tag build definition: https://hub.docker.com/layers/library/node/22.21.1-bookworm-slim/?tab=layers (arm64 build branch; not a verified child manifest)
- Nest core 11.1.8 published metadata: https://app.unpkg.com/@nestjs/core@11.1.8/files/package.json
- Nest common 11.1.8 published metadata: https://app.unpkg.com/@nestjs/common@11.1.8/files/package.json
- npm 10 release history: https://docs.npmjs.com/cli/v10/using-npm/changelog/
- Timescale official release history: https://github.com/timescale/timescaledb/releases

Evidence limits: direct registry/Maven/npm downloads were unavailable in this
session. Published documentation/package metadata establishes the specification;
installation, digest resolution and downloaded-byte verification are explicitly
unperformed. This document is not an audit of current CVEs or a production
security approval.
