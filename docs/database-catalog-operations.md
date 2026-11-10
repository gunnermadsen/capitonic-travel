# Database and catalog operations

Run from the repository root after the initial identities and storage setup in
[local-development.md](local-development.md). These are operator commands, not
custom initialization jobs. Never regenerate an existing CA or overwrite existing
credentials. Application packages, migrations, and tables remain deferred.

## Identities and private state

The initial identity loop in local-development includes the identities below.
For an existing foundation, issue only missing identities using that procedure
and preserve all existing CAs and keys. Use a separate
`postgres` CA (RSA 3072, SHA-256, 1095 days), and these 90-day leaves:

| Identity/CN suffix | CA | Extended usage | SANs |
| --- | --- | --- | --- |
| postgres-server | postgres | serverAuth | DNS:postgres,DNS:localhost,IP:127.0.0.1 |
| postgres-bootstrap-admin | postgres | clientAuth | DNS:postgres-bootstrap-admin |
| database-migration | postgres | clientAuth | DNS:database-migration |
| connection-api-db | postgres | clientAuth | DNS:connection-api-db |
| polaris-bootstrap-db | postgres | clientAuth | DNS:polaris-bootstrap-db |
| polaris-db | postgres | clientAuth | DNS:polaris-db |
| postgres-denied | postgres | clientAuth | DNS:postgres-denied |
| polaris-server | infrastructure | serverAuth | DNS:polaris,DNS:localhost,IP:127.0.0.1 |
| flink-kafka | kafka | clientAuth | DNS:flink-kafka |

Every CN is prefixed with `airline-dev.`. The unmapped `postgres-denied` leaf is
only for negative verification. `pg_ident.conf` is authoritative for role maps.
The Java PostgreSQL driver requires PKCS12 alias `user`:

```bash
set -eu
umask 077
for identity in polaris-bootstrap-db polaris-db; do
  openssl pkcs12 -export -name user \
    -inkey ".local/identity/$identity/tls.key" \
    -in ".local/identity/$identity/tls.crt" \
    -certfile .local/identity/ca/postgres/ca.crt \
    -passout pass: -out ".local/identity/$identity/client.p12"
done
```

Generate these files once, failing if the destination already exists. Keep all
files mode 0600 and private directories 0700:

- `.local/secrets/postgres-password`: `openssl rand -hex 24`. This bootstraps the
  local superuser; remote access still requires a mapped certificate.
- `.local/secrets/polaris-signing.key`: `openssl genpkey -algorithm RSA -pkeyopt
  rsa_keygen_bits:3072`; export its public key with `openssl pkey -pubout` to
  `polaris-signing.pub`. Persist this pair across restarts.
- `.local/secrets/polaris-bootstrap.json`: JSON object
  `{"airline-dev":{"client-id":"airline-bootstrap-admin","client-secret":"<random hex>"}}`.
- `polaris-s3.env` and `archive-s3.env`: `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_REGION=us-east-1`; use distinct users
  `airline-dev-polaris-storage` and `airline-dev-flink-archive`, with random secrets.
- `flink-archive.env`: copy the archive values under `ICEBERG_S3_ACCESS_KEY` and
  `ICEBERG_S3_SECRET_KEY`. Do not override Flink's separate checkpoint AWS identity.

Provision both warehouse users with the RustFS `add-user` API and attach
`common/configs/rustfs/warehouse-policy.json`, using the same signed-curl/`rc`
procedure as the state user in local-development. The policy is named
`airline-warehouse`. It grants only `airline-dev-warehouse` access; neither
warehouse user can access checkpoint/savepoint buckets.

Stage only the following files under `.local/runtime`, using a root tool container
to set the verified numeric owner. Never mount CA private keys:

| Runtime directory | Owner | Files |
| --- | --- | --- |
| postgres | 70:70 | server `tls.crt`, `tls.key`, postgres `ca.crt`, bootstrap `password` |
| polaris-bootstrap | 10000:10001 | `postgres-ca.crt`, bootstrap-db `client.p12`, `credentials.json` |
| polaris | 10000:10001 | `postgres-ca.crt`, runtime-db `client.p12`, server `tls.crt`/`tls.key`, infrastructure `https-ca.crt`, `signing.key`/`signing.pub`, `https-trust.jks` |
| each Flink runtime and SQL client | 9999:9999 | dedicated Flink Kafka `kafka.p12` and Kafka `kafka-trust.p12`, in addition to existing identities |

`https-trust.jks` is a copy of the existing public infrastructure CA truststore;
its owner-protected, read-only mount permits loading without a password in JVM
arguments. Use the existing store password for the Kafka PKCS12 files. The empty
PostgreSQL PKCS12 password is protected by file ownership and the read-only mount.

## Startup and database boundaries

```bash
docker compose --env-file .local/secrets/compose.env build postgres flink-jobmanager
docker compose --env-file .local/secrets/compose.env up -d postgres rustfs kafka
docker compose --env-file .local/secrets/compose.env run --rm polaris-bootstrap
docker compose --env-file .local/secrets/compose.env up -d polaris flink-jobmanager flink-taskmanager
```

Postgres runs `common/configs/postgres/bootstrap.sql` only on a new data volume.
It creates the application and catalog databases, restricted roles, owned schemas,
and TimescaleDB extension. No application tables or hypertables are created.
`airline_migrator` owns the application schema; `airline_runtime` has only schema
usage until future migrations grant table-specific permissions. `polaris_owner`
owns vendor metadata; `polaris_runtime` has DML but no DDL rights. Neither runtime
role is a superuser, database creator, role creator, or replication role.

The pinned Polaris admin tool initializes its vendor schema and realm; repeating
it skips an existing realm. Polaris 1.8 does not accept the old `--schema-version`
argument. Fresh bootstrap selects schema v6. An existing schema needs an explicit
operator upgrade; the server does not migrate it automatically.

For the existing v3 installation, a protected `pg_dumpall` was captured first.
The reviewed v3-to-v6 change ran in one transaction as `polaris_owner`: make
`events.catalog_id` nullable, replace legacy `__realm__` values with NULL, drop
`idx_locations`, then apply the unmodified vendor
`apache-polaris-1.8.0/persistence/relational-jdbc/src/main/resources/postgres/schema-v6.sql`
with `search_path=polaris_schema`. This recreates the index, adds metrics tables
and indexes, and records version 6 without replacing realm data. This is vendor
metadata maintenance, never an application migration.

The PG17 minor update preserves its data directory. An existing TimescaleDB
installation requires `ALTER EXTENSION timescaledb UPDATE TO '2.29.1'` as the
first statement in a fresh privileged session after replacing the image. Do not
run ordinary extension queries first. On a fresh volume, bootstrap installs the
current extension directly.

Postgres publishes only `127.0.0.1:15432`. The additional `database-host` bridge
is required by Docker for loopback port publication; the catalog database network
remains internal. Remote HBA rules require certificate authentication and reject
plaintext. Clients must use `sslmode=verify-full`, a trusted CA, and a matching
hostname. Example:

```bash
PGSSLMODE=verify-full PGCONNECT_TIMEOUT=5 \
PGSSLROOTCERT="$PWD/.local/identity/ca/postgres/ca.crt" \
PGSSLCERT="$PWD/.local/identity/connection-api-db/tls.crt" \
PGSSLKEY="$PWD/.local/identity/connection-api-db/tls.key" \
psql -X -h localhost -p 15432 -U airline_runtime -d airline -c 'SELECT current_user'
```

## Catalog registration and SQL verification

Polaris publishes HTTPS at `https://localhost:18181`; container clients use
`https://polaris:8443`. Supply `Polaris-Realm: airline-dev` on every API request.
Exchange the bootstrap client credentials at `/api/catalog/v1/oauth/tokens` using
HTTP Basic authentication, `grant_type=client_credentials`, and
`scope=PRINCIPAL_ROLE:ALL`. Keep credentials and tokens in private curl config
files, not shell arguments or logs. Verify the infrastructure CA; never use `-k`.

Register `airline` with `POST /api/management/v1/catalogs`:

```json
{"catalog":{"name":"airline","type":"INTERNAL","readOnly":false,"properties":{"default-base-location":"s3://airline-dev-warehouse/"},"storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://airline-dev-warehouse/"],"region":"us-east-1","endpoint":"https://rustfs:9000","pathStyleAccess":true,"stsUnavailable":true}}}
```

Create principal `flink-archive` (`POST /principals`) and persist the returned
credentials privately. Create principal role `flink-archive` and catalog role
`archive-writer` within `airline`. Assign the principal role to the principal,
assign the catalog role to that principal role, and grant
`{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}` through
`PUT /catalogs/airline/catalog-roles/archive-writer/grants`. These are management
API paths under `/api/management/v1`. Flink receives no account/catalog access
administration grants or bootstrap credentials. Check for existing resources on
repeat; do not silently rotate or recreate identities.

Run vendor SQL Client in batch mode with tableau results and `table.dml-sync=true`.
Create a REST Iceberg catalog using these properties:

| Property | Value |
| --- | --- |
| type / catalog-type | iceberg / rest |
| uri | https://polaris:8443/api/catalog |
| warehouse | airline |
| oauth2-server-uri | https://polaris:8443/api/catalog/v1/oauth/tokens |
| credential | dedicated Flink client ID and secret separated by `:` |
| scope / header.Polaris-Realm | PRINCIPAL_ROLE:ALL / airline-dev |
| io-impl | org.apache.iceberg.aws.s3.S3FileIO |
| s3.endpoint / s3.path-style-access | https://rustfs:9000 / true |
| client.region | us-east-1 |
| s3.access-key-id / s3.secret-access-key | dedicated archive storage identity |

Put credential-bearing SQL under `.local/secrets`, mount only that file into the
SQL client, and redirect its echoed output there too. Do not request credential
vending: RustFS uses the separately provisioned, warehouse-scoped credentials.

Create a disposable namespace/table, insert known rows, and run a separate SELECT
to prove a committed snapshot can be read. Recreate the services without `-v`,
query the same rows again, then drop the table and namespace through Flink SQL. Flink sends purge-style
drops: temporarily set catalog property `polaris.config.drop-with-purge.enabled`
to `true` through the management API (with its current entity version), perform
the disposable drop, and restore `false`. It remains disabled by default.
Do not confuse a successful checkpoint or INSERT submission with a readable
Iceberg commit. Verify the catalog remains registered after cleanup.

## Upgrade checks

Kafka remains a single combined KRaft broker/controller; no ZooKeeper service or
dependency exists. Preserve the cluster ID and named volume. Back up the stopped
Kafka volume before a major upgrade. Verify the quorum with
`kafka-metadata-quorum.sh ... describe --status`, then finalize `metadata.version`
only after successful connector reads/writes. The verified Kafka 4.4 metadata
level is 33. Static quorum configuration retains `kraft.version=0` intentionally.

Flink 2.2.1 uses Kafka connector 5.0.0-2.2 and Iceberg 1.12.0. Checkpoint paths
remain in RustFS. The bundled 2.2 example uses newer state APIs and requires
`flink savepoint --type native`; canonical savepoints are unsupported for this
example. Restore with the same 2.2.1 example and verify new completed checkpoints.
An old 1.20 example savepoint is retained as history, not claimed compatible with
the changed 2.2 example. No application job state existed at upgrade time.

Sources: [Polaris relational JDBC lifecycle](https://polaris.apache.org/releases/1.8.0/metastores/relational-jdbc/),
[Polaris 1.8 changes](https://polaris.apache.org/downloads/1.8.0/),
[Flink connector matrix](https://flink.apache.org/downloads/).
