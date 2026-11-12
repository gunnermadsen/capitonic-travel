# Confluent teardown

These opt-in Terraform overrides enable deletion only when explicitly copied
into their owning roots. Normal provisioning keeps `prevent_destroy` enabled.
They use the existing state, never a second copy of the managed resources.
Do not delete state or revoke the bootstrap Cloud credentials before cleanup.

Stop ingestion first. Do not remove Docker volumes: they contain the durable
AeroAPI request count and checkpoints.

```sh
docker compose -f docker-compose.confluent.yml stop ingester
cp common/terraform/teardown/application_override.tf.example common/terraform/confluent/teardown_override.tf
terraform -chdir=common/terraform/confluent plan -destroy \
  -target=confluent_service_account.provisioner -out=application-destroy.tfplan
```

Review the plan. It must delete exactly the managed provisioning account, its
cluster role, both managed Kafka keys, four topics and seven ACLs, plus the
Schema Registry key, four subject roles and four registered schema versions
(24 resources).
The destroy target includes resources depending on the provisioning account.
It must NOT delete existing `capitonic-sa` (`sa-v1wnjrz`), the environment, or
the existing Schema Registry, or unrelated resources. The imported account's
`prevent_destroy` remains enabled. Schema teardown permanently removes the four
tracked versions; it does not sweep unrelated subjects or unmanaged versions.
Topic deletion
permanently deletes their records. Apply only after accepting that data loss:

```sh
terraform -chdir=common/terraform/confluent apply application-destroy.tfplan
rm common/terraform/confluent/teardown_override.tf
```

Terraform's dependency order retains the administrative key and role until topic
and ACL deletion finishes. Registry credentials and subject roles similarly remain
until schema deletion finishes. Only after that apply succeeds, destroy the cluster:

```sh
cp common/terraform/teardown/cluster_override.tf.example common/terraform/confluent-cluster/teardown_override.tf
terraform -chdir=common/terraform/confluent-cluster plan -destroy -out=cluster-destroy.tfplan
```

Review that only `confluent_kafka_cluster.flights` is deleted. Cluster deletion
also deletes any unmanaged contents added manually; verify there are none to
retain before applying:

```sh
terraform -chdir=common/terraform/confluent-cluster apply cluster-destroy.tfplan
rm common/terraform/confluent-cluster/teardown_override.tf
```

After cleanup, clear `CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET` in `.env`;
retain bootstrap Cloud credentials only if still needed. Update `KAFKA_BROKER`
and the schema IDs in `common/compose/confluent.yaml` before starting against a
reprovisioned cluster. Securely
remove obsolete saved plan files and protect state backups, which may contain
revoked secrets. Preserve the current state files for safe subsequent provisioning.
If cancelling teardown, remove the copied overrides and saved destroy plans.
Do not run a full application `terraform destroy`: the imported existing account
is deliberately protected and retained.
