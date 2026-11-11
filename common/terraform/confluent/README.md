# Confluent topics and access policies

This configuration manages four topics from the existing
[`manifest.json`](../../configs/kafka/topics/manifest.json) and seven literal ACLs
for `capitonic-sa` (`sa-v1wnjrz`) on the cluster provisioned by [`confluent-cluster`](../confluent-cluster/README.md).
It also adopts the existing application service account and creates a separate
`capitonic-terraform` service account with `CloudClusterAdmin` on this cluster
only. It creates separate cluster-scoped provisioning and application API keys.
Cluster creation remains in the independent cluster workflow.

Topics retain the manifest's partition count and retention settings. Confluent
manages replication; local broker settings are not copied. Topic deletion and
replacement are blocked by `prevent_destroy`.

The account receives Describe/Write on `flight.updates` and
`ingestion.rejections`, cluster IdempotentWrite, and Read on `flight.updates` and
group `airline-archive-flights` for verification. The other two topics are created
without granting this ingester access to them. Existing unrelated ACLs and role
bindings are not removed.

## Credentials

Set `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET` in root `.env`
to the bootstrap administrator credentials. Terraform reads these exact names;
no aliases are required. Use literal `NAME=value` lines, optionally quoted.
Shell interpolation and inline comments are not supported here.

The provisioning Kafka key is managed in Terraform state and used only by the
provider. The ingester key is exposed through sensitive outputs named exactly
`CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET`; copy those into the same names in
`.env`, along with output `KAFKA_BROKER`. Never put the provisioning key in the
application environment. State and saved plans contain API secrets: preserve them
privately, with restricted permissions, outside source control.

## Run from the repository root

Requires Terraform 1.14 or later (before 2.0). Run the independent
[cluster workflow](../confluent-cluster/README.md) first. This configuration reads
its local state outputs for cluster IDs and endpoints; preserve both state files.

For the initial account bootstrap, the Cloud credentials can belong to your
existing administrator **user account** (API keys → Add API key → My account →
Cloud resource management). A service account is not required to obtain that
initial key. The credential owner must be authorized to create accounts and grant
the cluster role.

Run just account provisioning first:

```sh
terraform -chdir=common/terraform/confluent init
terraform -chdir=common/terraform/confluent plan \
  -target=confluent_service_account.ingester \
  -target=confluent_role_binding.provisioner_cluster_admin \
  -out=accounts.tfplan
terraform -chdir=common/terraform/confluent apply accounts.tfplan
```

The import block adopts `sa-v1wnjrz`; its existing keys and description are
preserved. Both service accounts have `prevent_destroy`. Repeat the same plan
after applying to verify no account or role changes remain. Keep the state:
account names alone do not prevent duplicate creation if state is lost.
Targeting is only for this explicit account-first bootstrap; use full plans for
normal infrastructure reconciliation.

Bootstrap the provisioning key once before the full plan, because the provider
requires known Kafka credentials at plan time:

```sh
terraform -chdir=common/terraform/confluent plan -target=confluent_api_key.provisioner -out=credentials.tfplan
terraform -chdir=common/terraform/confluent apply credentials.tfplan
```

Then provision topics, ACLs and the application key:

```sh
terraform -chdir=common/terraform/confluent init
terraform -chdir=common/terraform/confluent validate
terraform -chdir=common/terraform/confluent plan -out=confluent.tfplan
terraform -chdir=common/terraform/confluent apply confluent.tfplan
```

Review the plan before applying. If a topic already exists, import it first, for
example:

```sh
terraform -chdir=common/terraform/confluent import \
  'confluent_kafka_topic.topics["flight.updates"]' \
  '<cluster_id>/flight.updates'
```

Existing matching ACLs can likewise be imported using the provider's
[ACL import format](https://registry.terraform.io/providers/confluentinc/confluent/2.94.0/docs/resources/kafka_acl#import).
Compose starts the application only; it does not run Terraform. Local Compose
continues to use the `kafka-topics` job and local certificate permissions.

After transferring the three application outputs to `.env`, start with
`docker compose -f docker-compose.confluent.yml up -d`. Set an explicit
`AEROAPI_MAX_REQUESTS` lifetime cap before starting; existing request counts are
preserved. A repeated full Terraform plan should report no changes.

For ordered cleanup that preserves the pre-existing application account, see
[teardown](../teardown/README.md).
