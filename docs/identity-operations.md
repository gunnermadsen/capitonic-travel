# Identity and mTLS operations

Existing certificates and CA material remain under ignored `.local/identity`;
runtime bundles remain under `.local/runtime`. Removing the custom certificate
utilities does not rotate or remove these files.

Identity names and certificate requirements are recorded in
`common/configs/identities/identities.json`. Runtime file mappings and numeric
owners are recorded in `common/configs/identities/runtime.json`; Compose defines
which files each container mounts.

Certificate provisioning, inspection, and rotation are operator tasks using
standard OpenSSL and Java keytool commands. The repository no longer supplies
certificate generation, staging, rotation, or connection-check commands.
Preserve existing CA private keys and service identities. Copy only the files
listed for a service into its runtime directory, with the manifest's UID/GID,
0600 private files, and 0700 directories. Do not overwrite an existing runtime
bundle as part of initial provisioning. Never mount CA private keys into services.

Use the existing operator setup instructions in [local development](local-development.md)
for infrastructure ownership and vendor tooling. Application job directories use
UID/GID 1000:1000. Verify certificate validity, SANs and trust before activation;
maintain overlapping trust when rotating authorities.

## Reload and restart requirements

- Kafka: this local deployment uses static mounted stores. Recreate the broker
  after updating its files; restart clients to reload their TLS material. Preserve
  KRaft cluster ID and volumes. A single broker incurs downtime.
- Flink: savepoint and stop active jobs, update stores, recreate JobManager and
  TaskManager, and resubmit from saved state. Restart SQL clients. Validate REST,
  internal registration, checkpoints, and Kafka paths. Do not assume live reload.
- PostgreSQL: update server PEM/trust files and call `pg_reload_conf()`; inspect
  logs and establish new sessions. Existing sessions retain their old TLS state.
  Restart client pools (including Polaris) after replacing client certificates.
- Polaris/RustFS: recreate affected services after HTTPS or Java trust changes;
  retain their data volumes and Polaris signing keys. Recheck authenticated APIs.
