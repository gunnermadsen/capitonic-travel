# Local infrastructure verification

Verified on 2026-11-08. Foundation and Phase 2 gates passed after the streaming
upgrade described below. Six infrastructure services remain running; no
application packages or custom initialization jobs were generated.

## Current versions and compatibility

| Component | Verified version |
| --- | --- |
| Kafka | 4.4.0, combined KRaft broker/controller; metadata 4.4-IV2 (33), static quorum |
| Flink | 2.2.1, Java 17, native ARM64; one TaskManager/one slot |
| Kafka SQL connector | 5.0.0-2.2 |
| Iceberg runtime / AWS bundle | 1.12.0, Flink 2.2 runtime, format v2 |
| Checkpoint plugin | flink-s3-fs-hadoop 2.2.1 |
| PostgreSQL / TimescaleDB | 17.11 / 2.29.1, native ARM64 Alpine build |
| Polaris server / admin tool | 1.8.0, persistent relational JDBC schema v6 |
| RustFS | 1.0.1 |

Flink 2.3.0 is newer, but the published Kafka connector matrix supports only
through Flink 2.2. The latest compatible stable selection is therefore 2.2.1.
Image child digests and artifact checksums are locked in Compose/Dockerfiles.
Build IDs: Flink `sha256:ed6d71550b73ddcfca908bb04f4396152c3a1c9455a9736400fd6b73f8983304`;
Postgres `sha256:a222cf4dae18d7bde4fa06b17eb57954f9b4823704e9c9b18d6505d98a82afe1`.

## Database/catalog and upgraded infrastructure gates

| Gate | Observed result |
| --- | --- |
| Kafka KRaft | Original cluster ID `cQZq574NRQWdV-O_50YXNQ` preserved; leader 1, one voter, zero follower lag after upgrade/recreation. No ZooKeeper. TLS-only listeners, client certificates, default-deny ACLs, and disabled topic autocreation retained. |
| Kafka/Flink connector compatibility | Dedicated Flink certificate and topic/group ACLs; SQL wrote and read `(7, kafka-4.4-flink-2.2)` through connector 5.0.0-2.2. Same record read after recreation. |
| Streaming authentication | Kafka and Flink REST accepted approved identities; rejected missing/untrusted certificates and wrong hostnames. Internal Flink RPC rejected a certificate-less handshake. Trusted unauthorized Kafka principal received TopicAuthorizationException and zero messages. |
| Flink internal security | TaskManager registered over `pekko.ssl.tcp`; authenticated REST reported Flink 2.2.1 and one available slot. |
| Checkpoints and recovery | Bundled 2.2 example wrote checkpoints to RustFS. Native savepoint `s3://airline-dev-savepoints/flink/savepoint-e98aee-dda30dff04b6` survived recreation. Restored job reported one restoration and nine completed new checkpoints at capture. |
| PostgreSQL certificates | All five mapped roles connected with verify-full. Server observed TLS 1.3. Missing, untrusted, unmapped, and mismatched-role certificates failed; plaintext and wrong-hostname clients failed. |
| Database boundaries | Separate airline/polaris databases and schemas. Runtime roles cannot create tables; application runtime cannot connect to catalog DB. TimescaleDB 2.29.1 enabled; application schema has zero tables and no application hypertables. |
| Polaris persistence lifecycle | Vendor schema upgraded transactionally from v3 to v6 after a protected dump. Vendor admin repeat skipped existing realm. Fresh bootstrap into an isolated schema created v6 successfully; disposable schema removed. |
| Polaris transport/authentication | HTTPS API; valid OAuth required (no-token 401). JDBC uses mapped runtime certificate, verify-full and TLS 1.3. Dedicated Flink principal received 403 for account administration. |
| Storage permissions | Separate state, archive, and Polaris identities. Archive identity received 403 for checkpoint bucket access; anonymous storage access received 403. HTTPS trusted infrastructure CA throughout. |
| Iceberg commit | Flink created a disposable namespace/table, inserted two rows, and independently queried `persistent-catalog` and `persistent-object`. Actual Parquet, manifest, and metadata objects existed in RustFS. |
| Container recreation | All six containers recreated with named volumes preserved. Kafka record, Postgres extension/roles, Polaris realm/catalog/schema, and both Iceberg rows survived. |
| Disposable cleanup | Flink dropped the Iceberg table and namespace; catalog remains registered. Catalog purge enabled only for cleanup, then disabled again. Kafka smoke topic and its test ACLs removed; stateful smoke job cancelled. |
| Declarative validation | Root Compose validation and native image builds passed; credentials remain under ignored .local; no application code generated. |

The bundled Flink 2.2 example uses newer state APIs: canonical savepoints fail
with UnsupportedOperationException, while native savepoints and restoration
passed. Its first startup checkpoint can be declined before tasks become ready;
subsequent checkpoints completed. This check does not claim the changed 2.2
example can restore a 1.20 example savepoint. No application job state existed.
Single-JobManager deployment still requires explicit resubmission after recreation.
Application consumers, Node-client protocol compatibility, transactional pipeline
semantics, and later-phase acceptance scenarios remain outside these gates.

Evidence is local under `.local/evidence/`: `database-gates.json`,
`streaming-tls-gates.json`, `kafka-final-quorum.txt`,
`kafka-read-after-recreation.txt`, `catalog-read-after-recreation.txt`,
`catalog-cleanup.txt`, `upgraded-restored-checkpoints.json`, and recreation logs.
Credential-bearing SQL and raw SQL output remain private under `.local/secrets/`;
only redacted copies belong in evidence.

Operations: [database/catalog](database-catalog-operations.md) and
[foundation](local-development.md). Sources: [Flink matrix](https://flink.apache.org/downloads/),
[Iceberg 1.12.0](https://iceberg.apache.org/blog/apache-iceberg-1.12.0-release/).

## Historical foundation verification

The sections below record the original 1.20/3.9 foundation and layout checks.
Current versions, capacity, and acceptance results above supersede those historical
runtime values. Current service memory ceilings total 6 GiB, plus one bounded tool.


## Implemented foundation

- `compose.yaml`: persistent Kafka, RustFS, Flink JobManager and
  TaskManager; isolated streaming/lake networks; loopback-only published ports;
  explicit memory/CPU limits; optional vendor-tool profiles.
- `common/configs/kafka/server.properties`: single-node KRaft with distinct broker and
  controller identities, TLS-only listeners, required client certificates,
  StandardAuthorizer/default deny, explicit bootstrap superusers, no automatic
  topic creation, persistent broker metadata/logs.
- `common/infra/flink/Dockerfile`: native ARM64 Flink 1.20.3, Java 17, checksum-pinned
  Kafka/Iceberg/AWS/Hadoop clients, isolated S3 checkpoint plugin. Final local
  image ID: `sha256:c607ac221a3f202c73db7de8877a9faa26fbb39fdf53078a78926029b7b93e04`.
- `common/configs/rustfs/flink-state-policy.json`: Flink can operate only on the checkpoint
  and savepoint buckets, not warehouse data or IAM administration.
- Ignored `.local/`: operator-issued CAs/leaves, private credentials, per-process
  read-only runtime identity material, and local verification evidence. CA private
  keys never enter a container. Original keys are host-owned; runtime keys are
  0600 and owned by the verified image UID.

## Gate evidence

| Gate | Observed result |
| --- | --- |
| Native reproducible infrastructure | Registry ARM64 child digests matched the selected versions; image build checksums passed; Compose configuration validated; all four services started and were recreated successfully. |
| Kafka authentication | Trusted admin leaf completed the host TLS handshake with hostname verification; no certificate and a Flink-issued certificate were rejected; a wrong hostname was rejected. |
| Kafka authorization and data flow | Vendor CLI created a disposable topic and produced/consumed its marker. A Kafka-CA-signed identity without permissions received `TopicAuthorizationException` and read zero messages. |
| RustFS authentication and isolation | Signed object write/read succeeded over verified HTTPS; anonymous read returned 403; scoped Flink credentials were denied a warehouse write. |
| Flink secure connectivity | TaskManager registered with `pekko.ssl.tcp` and TLS-enabled shuffle transport; REST reported one TaskManager/two slots. Internal RPC rejected a certificate-less connection. REST rejected missing and untrusted certificates. |
| Required connector loading | SQL client planned a Kafka `TableSourceScan`; created, listed and dropped a disposable local Iceberg catalog. No missing-class errors remained. This is not a claim of Polaris integration or Iceberg S3 commits. |
| Durable checkpoints/savepoints | Bundled `StateMachineExample.jar` completed 38 checkpoints with zero failures before the first savepoint. Savepoint metadata was readable in RustFS. |
| Container recreation | Kafka marker and S3 object were readable after forced recreation of all four services. Persisted RustFS IAM credentials/policy also remained effective. |
| Actual state restoration | Restored from `s3://airline-dev-savepoints/flink/savepoint-5818d2-417d0d91514e`. Final image reported one restoration and eight completed new checkpoints at capture, with an S3 external path. |
| Scope gate | No application packages, npm installation, business services, custom initialization jobs or certificate automation scripts were introduced. |

The example's restored runs each rejected one checkpoint while source tasks were
still starting (`Not all required tasks are currently running`), then continued
successfully. This is recorded rather than presenting the restored runs as having
zero rejected attempts. The example itself selects its checkpoint schedule.

Flink's HTTP listener returns a 301 HTTPS redirect, not plaintext API data. All
API access requires the REST CA and an authorized client certificate. Internal
trust is restricted to the dedicated runtime/SQL-client CA.

The disposable Kafka topic `smoke_phase1` and object `smoke_phase1.txt` used during
verification were removed. Those are historical test resource identifiers, not
project filenames. All submitted vendor example jobs were cancelled after their
checks. Small retained checkpoints and the savepoint remain as recovery evidence.
No persistent volumes were removed.

## Project layout verification

After moving Compose to the repository root, configurations to `common/configs/`,
and the Flink image to `common/infra/flink/`, the affected build, bind-mount,
secret-file and documentation paths were corrected. On 2026-11-08, Compose
validation and the relocated image build passed without changing the image ID.
Authenticated Kafka and S3 round trips, Flink registration (one TaskManager/two
slots), and SQL Kafka/Iceberg connector checks passed. Containers were refreshed
where their Docker metadata still referenced the previous Compose location;
existing named volumes were preserved. Disposable test resources were removed.

## Capacity and later-phase boundaries

Docker exposes 4 CPUs and 7.5 GiB RAM. Initial verification had only 1.5 GiB
free on a 7.8 GiB filesystem. After the Docker disk expansion, the project layout
verification measured a 56 GiB filesystem with 47 GiB free (12% used); the earlier
low-space limitation is resolved. Phase 1 retains its 5.75 GiB aggregate service
memory ceilings; initially observed idle service memory was about 1.35 GiB total.

Single-JobManager mode requires explicit resubmission from saved state after
JobManager recreation; it does not provide automatic HA recovery. At this historical checkpoint, Polaris,
Postgres, and Iceberg warehouse writes were not yet verified; see current gates above.

Reproduction commands and credential-handling details:
[Local infrastructure operations](local-development.md).
