# Local infrastructure operations

## Choose local infrastructure or Confluent Cloud

Build once with `npm run build`, then choose one deployment:

```sh
docker compose -f docker-compose.local.yml up -d
# or, after filling the Confluent values in .env:
docker compose -f docker-compose.confluent.yml up -d
```

Each entrypoint loads the shared application definitions and the selected deployment
configuration through native Compose `include`. No npm startup command, second
`-f` flag or `--env-file` argument is needed. Docker Compose 2.20.3 or later is
required. The local entrypoint loads `.env` and `.local/secrets/compose.env`; the
cloud entrypoint loads only `.env`.

`compose.yaml` holds shared application definitions; `common/compose/local.yaml`
and `common/compose/confluent.yaml` contain deployment-specific settings. Use the
root `docker-compose.*.yml` files as entrypoints. Local startup includes the database
migration job automatically; diagnostic tools remain opt-in.

When switching deployments, add `--remove-orphans` to the selected `up -d` command
to stop and remove containers from the previous mode. Plain `up -d` does not remove
already running services from another file. Both modes share the existing project
name and persistent volumes. No volumes are deleted and no Kafka history is moved.

For cloud mode, fill these exact names in root `.env`:

- `KAFKA_BROKER`: the cluster bootstrap hostname and port (without `https://`).
- `CONFLUENT_API_KEY`: a Kafka API key authorized for this cluster.
- `CONFLUENT_API_SECRET`: its secret.

Cloud Compose runs only the ingester. Provision the four topics and their ACLs
through [Terraform](../common/terraform/confluent/README.md) before starting it.
Compose does not execute Terraform or the Kafka topic job in
cloud mode. The topic job remains part of local startup only.

The cloud application key needs Describe/Write on `flight.updates` and
`ingestion.rejections`, plus cluster IdempotentWrite. It does not need topic-creation
permissions. Read-back verification additionally needs Read on `flight.updates` and
consumer group `airline-archive-flights`. Terraform uses Cloud management credentials to provision a separate
administrative Kafka key and the restricted application key; see its README for
the credential setup. TLS verifies public certificate roots; SASL/PLAIN sends
the application API key and secret over that encrypted connection.

The ingester checks Kafka connectivity at startup, but does not create topics.
Managed Flink, Schema Registry and Tableflow are not provisioned by Compose.

AeroAPI and Firehose selection remains unchanged: default AeroAPI, or set
`INGESTER_PRODUCT=flight-updates` with `INGESTER_MODE=PUBLIC` for Firehose.
`FLIGHTAWARE_API_KEY`, `FIREHOSE_USERNAME` and `FIREHOSE_API_KEY` retain their exact
names. Use `docker compose -f docker-compose.local.yml stop` or `docker compose -f docker-compose.confluent.yml stop` to stop that deployment.


The same ingester state volume is retained when switching, including request counts
and deduplication state. Previously acknowledged events are not republished to a
new cluster; pending events go to the selected cluster. AeroAPI polls continuously
at `AEROAPI_POLL_INTERVAL_MS` (default 300000); there is no lifetime request cap.
Use `INGESTER_OUTPUT=validate` to validate AeroAPI events without a Kafka connection.
This uses an isolated validation checkpoint and never marks events as published.

The commands below describe local infrastructure operations and require the
existing local identity material and `.local/secrets/` files. Cloud mode does not
require those files.

Scope: Kafka, Flink, RustFS, TimescaleDB/Postgres, and Polaris. No application packages, application topics,
custom initialization jobs or certificate-generation scripts. Database/catalog
operator steps are in [database-catalog-operations.md](database-catalog-operations.md). The commands below
are operator commands, run from the repository root in Bash with `set -eu` and
`umask 077`. Do not run them against another environment.

## Prerequisites and resource limits

Docker Compose, native ARM64 Docker, OpenSSL, Java `keytool`, curl and Python 3
are required. The selected images are pinned to ARM64 child digests in Compose.
The Flink Dockerfile pins and checks all added connector artifacts. Kafka's SHA-256
was calculated from the Maven Central HTTPS artifact (that publication supplies
SHA-1, not SHA-512); Iceberg SHA-512 values came from Maven Central sidecars.
The Hadoop client API/runtime 3.4.2 pair is also SHA-256 locked from Maven Central;
Iceberg’s catalog factory requires these classes even though state storage uses
Flink’s separately isolated Hadoop S3 plugin.

This host's Docker VM has 4 CPUs and 7.5 GiB RAM. Service memory ceilings total
6 GiB: Kafka 1280 MiB/512 MiB heap, JobManager 1024 MiB/768 MiB process memory,
TaskManager 1792 MiB/1536 MiB process memory with one slot, RustFS 768 MiB,
Postgres 512 MiB, Polaris 768 MiB. Run one ephemeral tool container at a time;
the SQL client is capped at 512 MiB with a 256 MiB heap. Stop the bundled streaming
smoke job before SQL tests so its slot is available. Compose is authoritative for
CPU quotas and all resource limits.

The Docker VM disk was expanded after the initial foundation checks. The project
layout verification on 2026-11-08 measured a 56 GiB filesystem with 47 GiB free
(12% used), resolving the previous low-space limitation. Continue monitoring
usage against the project storage budget; do not prune unrelated resources.

## Initial identities (manual foundation reference)

Phase 4 adds manifest-driven utilities in [identity-operations.md](identity-operations.md).
Use those for normal generation, inspection, staging, and rotation. The original
operator commands below remain a foundation bootstrap reference; never rerun
manual generation over existing material.

Stop if any CA already exists. Keep `.local` outside source control. This
bootstrap deliberately fails on repeat rather than rotating identities or
replacing persistent credentials. It is not the Phase 4 lifecycle automation.

```bash
set -eu
umask 077
test ! -e .local/identity/ca
mkdir -p .local/identity .local/secrets .local/runtime .local/downloads .local/evidence
chmod 700 .local .local/identity .local/secrets .local/runtime
for domain in kafka flink-internal flink-rest infrastructure postgres; do
  mkdir -p ".local/identity/ca/$domain"
  openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 1095 \
    -subj "/CN=airline-dev.$domain.ca" \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' \
    -keyout ".local/identity/ca/$domain/ca.key" \
    -out ".local/identity/ca/$domain/ca.crt"
done
openssl rand -hex 24 > .local/secrets/store-password
while read -r name domain san eku; do
  mkdir -p ".local/identity/$name"
  openssl req -new -newkey rsa:3072 -nodes -subj "/CN=airline-dev.$name" \
    -keyout ".local/identity/$name/tls.key" -out ".local/identity/$name/tls.csr"
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=%s\nsubjectAltName=%s\n' \
    "$eku" "$san" > ".local/identity/$name/extensions.cnf"
  openssl x509 -req -sha256 -days 90 -in ".local/identity/$name/tls.csr" \
    -CA ".local/identity/ca/$domain/ca.crt" -CAkey ".local/identity/ca/$domain/ca.key" \
    -set_serial "0x$(openssl rand -hex 16)" -extfile ".local/identity/$name/extensions.cnf" \
    -out ".local/identity/$name/tls.crt"
  cp ".local/identity/ca/$domain/ca.crt" ".local/identity/$name/ca.crt"
  openssl pkcs12 -export -name "$name" -inkey ".local/identity/$name/tls.key" \
    -in ".local/identity/$name/tls.crt" -certfile ".local/identity/$name/ca.crt" \
    -passout file:.local/secrets/store-password -out ".local/identity/$name/keystore.p12"
  keytool -importcert -noprompt -alias "$domain" -file ".local/identity/$name/ca.crt" \
    -keystore ".local/identity/$name/truststore.p12" -storetype PKCS12 \
    -storepass:file .local/secrets/store-password
done <<'IDENTITIES'
kafka-broker-1 kafka DNS:kafka,DNS:localhost,IP:127.0.0.1 serverAuth,clientAuth
kafka-controller-1 kafka DNS:kafka serverAuth,clientAuth
kafka-bootstrap-admin kafka DNS:kafka-tools clientAuth
kafka-denied kafka DNS:kafka-denied clientAuth
flink-jobmanager-internal flink-internal DNS:flink-jobmanager serverAuth,clientAuth
flink-taskmanager-internal flink-internal DNS:flink-taskmanager serverAuth,clientAuth
flink-sql-internal flink-internal DNS:flink-sql-client serverAuth,clientAuth
flink-jobmanager-rest flink-rest DNS:flink-jobmanager,DNS:localhost,IP:127.0.0.1 serverAuth,clientAuth
flink-taskmanager-rest flink-rest DNS:flink-taskmanager serverAuth,clientAuth
flink-operator flink-rest DNS:flink-operator clientAuth
flink-sql-rest flink-rest DNS:flink-sql-client,DNS:localhost,IP:127.0.0.1 serverAuth,clientAuth
rustfs infrastructure DNS:rustfs,DNS:localhost,IP:127.0.0.1 serverAuth
postgres-server postgres DNS:postgres,DNS:localhost,IP:127.0.0.1 serverAuth
postgres-bootstrap-admin postgres DNS:postgres-bootstrap-admin clientAuth
database-migration postgres DNS:database-migration clientAuth
connection-api-db postgres DNS:connection-api-db clientAuth
polaris-bootstrap-db postgres DNS:polaris-bootstrap-db clientAuth
polaris-db postgres DNS:polaris-db clientAuth
postgres-denied postgres DNS:postgres-denied clientAuth
polaris-server infrastructure DNS:polaris,DNS:localhost,IP:127.0.0.1 serverAuth
flink-kafka kafka DNS:flink-kafka clientAuth
IDENTITIES
keytool -importcert -noprompt -alias infrastructure -file .local/identity/rustfs/ca.crt \
  -keystore .local/identity/rustfs/s3-trust.jks -storetype JKS \
  -storepass:file .local/secrets/store-password
printf 'password=%s\n' "$(cat .local/secrets/store-password)" > .local/secrets/password.properties
printf 'RUSTFS_ACCESS_KEY=%s\nRUSTFS_SECRET_KEY=%s\n' \
  "$(openssl rand -hex 12)" "$(openssl rand -hex 24)" > .local/secrets/rustfs.env
printf 'AWS_ACCESS_KEY_ID=airline-dev-flink-state\nAWS_SECRET_ACCESS_KEY=%s\nAWS_REGION=us-east-1\n' \
  "$(openssl rand -hex 24)" > .local/secrets/flink-s3.env
printf 'STORE_PASSWORD=%s\nCLUSTER_ID=%s\n' "$(cat .local/secrets/store-password)" \
  "$(openssl rand -base64 16 | tr '+/' '-_' | tr -d '=\n')" > .local/secrets/compose.env
```

## Stage only each process's identity material

Host-owned originals remain available to operators; the runtime copies belong to
verified image UIDs: Kafka 1000, Flink 9999, RustFS 10001. No CA private key is
mounted. The Flink internal CA is exclusive to the runtime and SQL operator;
REST operators use a separate CA. JKS S3 trust contains only a public CA and is
loaded without an integrity password from a read-only, owner-protected mount.
Never put a password in `JAVA_TOOL_OPTIONS`: Java prints that variable on startup.

```bash
python3 - <<'PY'
from pathlib import Path
import shutil, os
os.umask(0o077)
root = Path('.local/runtime')
for name in ['kafka', 'kafka-tools', 'rustfs', 'flink-jobmanager', 'flink-taskmanager', 'flink-sql-client']:
    (root/name).mkdir(mode=0o700, exist_ok=True)
def copy(source, target):
    shutil.copyfile(source, target)
    target.chmod(0o600)
for source, target in [('kafka-broker-1/keystore.p12','broker.p12'), ('kafka-controller-1/keystore.p12','controller.p12'), ('kafka-broker-1/truststore.p12','truststore.p12')]:
    copy('.local/identity/'+source, root/'kafka'/target)
copy('.local/secrets/password.properties', root/'kafka/password.properties')
password = Path('.local/secrets/store-password').read_text().strip()
client = ('security.protocol=SSL\nssl.endpoint.identification.algorithm=https\n'
          'ssl.keystore.type=PKCS12\nssl.truststore.type=PKCS12\n'
          'ssl.keystore.location=/run/identity/{store}\n'
          'ssl.keystore.password='+password+'\nssl.key.password='+password+'\n'
          'ssl.truststore.location=/run/identity/truststore.p12\n'
          'ssl.truststore.password='+password+'\nrequest.timeout.ms=5000\ndefault.api.timeout.ms=10000\n')
(root/'kafka/client.properties').write_text(client.format(store='broker.p12'))
for name in ['keystore.p12','truststore.p12']:
    copy('.local/identity/kafka-bootstrap-admin/'+name, root/'kafka-tools'/name)
(root/'kafka-tools/client.properties').write_text(client.format(store='keystore.p12'))
copy('.local/identity/kafka-denied/keystore.p12', root/'kafka-tools/denied.p12')
(root/'kafka-tools/denied.properties').write_text(client.format(store='denied.p12'))
for service, internal, rest in [('flink-jobmanager','flink-jobmanager-internal','flink-jobmanager-rest'), ('flink-taskmanager','flink-taskmanager-internal','flink-taskmanager-rest'), ('flink-sql-client','flink-sql-internal','flink-sql-rest')]:
    for source, target in [(internal+'/keystore.p12','internal.p12'), (internal+'/truststore.p12','internal-trust.p12'), (rest+'/keystore.p12','rest.p12'), (rest+'/truststore.p12','rest-trust.p12'), (rest+'/ca.crt','rest-ca.crt'), (rest+'/tls.crt','rest.crt'), (rest+'/tls.key','rest.key'), ('rustfs/s3-trust.jks','s3-trust.jks')]:
        copy('.local/identity/'+source, root/service/target)
for source, target in [('tls.crt','rustfs_cert.pem'), ('tls.key','rustfs_key.pem'), ('ca.crt','ca.crt')]:
    copy('.local/identity/rustfs/'+source, root/'rustfs'/target)
PY
docker run --rm --user 0 --entrypoint /bin/bash -v "$PWD/.local/runtime:/runtime" \
  apache/kafka:4.4.0@sha256:77b56ddfb7104fc5814a3d0127fcff49983220c7c7ad9bfc1f0af4d439228329 -ec \
  'chown -R 1000:1000 /runtime/kafka /runtime/kafka-tools
   chown -R 9999:9999 /runtime/flink-jobmanager /runtime/flink-taskmanager /runtime/flink-sql-client
   chown -R 10001:10001 /runtime/rustfs'
docker compose -f docker-compose.local.yml config --quiet
docker compose -f docker-compose.local.yml build flink-jobmanager
docker compose -f docker-compose.local.yml up -d --wait kafka rustfs
```

## Bootstrap authenticated storage with vendor tooling

Install the pinned RustFS CLI locally, not globally:

```bash
curl -fSL --retry 3 https://github.com/rustfs/cli/releases/download/v0.1.30/rustfs-cli-macos-arm64-v0.1.30.tar.gz \
  -o .local/downloads/rc.tar.gz
printf '%s  %s\n' '38c549a59608f906411166a824c1fe42c86cdd92710ffadcb0f8d5babf83d5a0' '.local/downloads/rc.tar.gz' | shasum -a 256 -c -
tar -xzf .local/downloads/rc.tar.gz -C .local/downloads rc
.local/downloads/rc --version
python3 - <<'PY'
from pathlib import Path
import json, os
os.umask(0o077)
p = Path('.local/secrets/rc'); p.mkdir(exist_ok=True)
s = dict(line.split('=',1) for line in Path('.local/secrets/rustfs.env').read_text().splitlines())
f = dict(line.split('=',1) for line in Path('.local/secrets/flink-s3.env').read_text().splitlines())
ca = str(Path('.local/identity/rustfs/ca.crt').resolve())
text = 'schema_version = 1\n'
for name, access, secret in [('local',s['RUSTFS_ACCESS_KEY'],s['RUSTFS_SECRET_KEY']), ('state',f['AWS_ACCESS_KEY_ID'],f['AWS_SECRET_ACCESS_KEY'])]:
    text += '\n[[aliases]]\n'
    for k,v in dict(name=name,endpoint='https://localhost:19000',access_key=access,secret_key=secret,region='us-east-1',signature='v4',bucket_lookup='path',ca_bundle=ca).items():
        text += k+' = '+json.dumps(v)+'\n'
    text += 'insecure = false\n'
(p/'config.toml').write_text(text)
Path('.local/secrets/s3-admin.curl').write_text('user = "'+s['RUSTFS_ACCESS_KEY']+':'+s['RUSTFS_SECRET_KEY']+'"\naws-sigv4 = "aws:amz:us-east-1:s3"\ncacert = "'+ca+'"\n')
Path('.local/secrets/state-user.json').write_text(json.dumps({'secretKey':f['AWS_SECRET_ACCESS_KEY'],'status':'enabled'}))
PY
export RC_CONFIG_DIR="$PWD/.local/secrets/rc"
.local/downloads/rc ready local
for bucket in airline-dev-checkpoints airline-dev-savepoints airline-dev-warehouse; do
  .local/downloads/rc bucket create "local/$bucket"
done
curl --fail --silent --show-error --config .local/secrets/s3-admin.curl \
  --request PUT -H 'Content-Type: application/json' --data-binary @.local/secrets/state-user.json \
  'https://localhost:19000/rustfs/admin/v3/add-user?accessKey=airline-dev-flink-state'
.local/downloads/rc admin policy create local flink-state common/configs/rustfs/flink-state-policy.json
.local/downloads/rc admin policy attach local flink-state --user airline-dev-flink-state
docker compose -f docker-compose.local.yml up -d --wait flink-jobmanager flink-taskmanager
```

The user-creation request follows RustFS CLI 0.1.30's vendor API. curl reads its
SigV4 credentials and JSON body from private files because the CLI's `user add`
accepts secrets only as positional arguments. Subsequent bucket/object/policy
checks use `rc`. Root credentials are never given to Flink.

## Normal lifecycle

After bootstrap, preserve `.local` and all named volumes. Start with:

```bash
docker compose -f docker-compose.local.yml up -d --wait
docker compose -f docker-compose.local.yml ps
```

Stop with the same Compose command followed by `down` **without `-v`**.
Recreation uses `up -d --force-recreate --wait`. Do not regenerate the cluster ID,
credentials, or CAs on restart. Do not run volume removal or bucket purges.
Single-JobManager mode does not automatically recover running jobs after loss of
the JobManager; explicitly restore a saved job from its checkpoint/savepoint.
HA recovery and application job lifecycle belong to later phases.

Endpoints: Kafka `localhost:19092`, Flink `https://localhost:18081`, RustFS S3
`https://localhost:19000`, RustFS console `https://localhost:19001`. All host
ports bind only to 127.0.0.1. Flink's REST listener redirects plaintext requests
with HTTP 301 to HTTPS; it does not return API data without client authentication.

Use the operator certificate for REST checks:

```bash
curl --fail --cacert .local/identity/flink-operator/ca.crt \
  --cert .local/identity/flink-operator/tls.crt --key .local/identity/flink-operator/tls.key \
  https://localhost:18081/overview
```

Expected: one TaskManager and two total slots. A running container alone does not
prove TaskManager registration. Run SQL tooling only when needed:

```bash
docker compose -f docker-compose.local.yml run --rm flink-sql-client
```

## Gate verification

Use a disposable topic such as `smoke_infrastructure`, never business topics. The Kafka
vendor tools run through the `kafka-tools` profile, with operator identity only:

```bash
docker compose -f docker-compose.local.yml run --rm kafka-tools -ec '
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --command-config /run/identity/client.properties --create --topic smoke_infrastructure --partitions 1 --replication-factor 1
  printf "infrastructure-persistence\n" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --producer.config /run/identity/client.properties --topic smoke_infrastructure
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --consumer.config /run/identity/client.properties --topic smoke_infrastructure --from-beginning --max-messages 1 --timeout-ms 10000'
```

Repeat consumption after recreation. With `denied.properties`, expect
`TopicAuthorizationException` and zero messages (the vendor console consumer can
exit zero even on authorization failure; inspect its result). With OpenSSL
`s_client`, verify the HOST listener accepts the Kafka admin leaf and rejects no
client certificate, a Flink-issued certificate, and incorrect server hostname.
Use `-verify_return_error -verify_hostname localhost -tls1_2` and the Kafka CA.
Perform the equivalent missing/untrusted certificate checks against Flink REST.

Use `rc object copy` and `rc object show` for a disposable state object; the state
identity must be denied a write to `airline-dev-warehouse`, and an unsigned curl
read of the state object must return 403. Re-read after RustFS recreation.

Run the bundled Flink example (no authored job code):

```bash
docker compose -f docker-compose.local.yml exec -T flink-jobmanager \
  /opt/flink/bin/flink run -d /opt/flink/examples/streaming/StateMachineExample.jar --error-rate 0 --sleep 1000
```

Record its job ID. Query `/jobs/<job-id>/checkpoints` with the operator REST
certificate and require completed checkpoints with an S3 external path. Create
a savepoint using `flink savepoint --type native <job-id> s3://airline-dev-savepoints/flink/`.
Cancel only that smoke job. Recreate all four services, verify the Kafka message
and S3 objects still exist, and use `flink run -s <savepoint-path> -d` with the
same example to verify saved state can actually be restored. Cancel that restored
smoke job before completing the gate. Delete only the named disposable topic and
smoke object after verification; never recursively delete checkpoint buckets.

## Upstream references

- [Flink 2.2 TLS](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/security/security-ssl/)
- [Flink S3 filesystem](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/filesystems/s3/)
- [RustFS TLS](https://docs.rustfs.com/en/integration/tls-configured)
- [RustFS CLI](https://docs.rustfs.com/en/operations/rc)
- [RustFS CLI 0.1.30 admin API implementation](https://github.com/rustfs/cli/blob/v0.1.30/crates/s3/src/admin.rs)
