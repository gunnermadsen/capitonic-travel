# Confluent cluster provisioning

Independent Terraform root and state for the Capatonic Flights Kafka cluster:
Standard, GCP `us-central1` (Iowa), `LOW` availability (99.9% SLA), maximum
one eCKU, in existing environment `env-7137m1`. Storage and traffic are billed
separately from compute; the eCKU ceiling is not a spending cap.

Uses `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET` directly from the
root `.env`, with the same literal-value format as account provisioning.
The credential owner must be authorized to create clusters in this environment.

From the repository root:

```sh
terraform -chdir=common/terraform/confluent-cluster init
terraform -chdir=common/terraform/confluent-cluster plan -out=cluster.tfplan
terraform -chdir=common/terraform/confluent-cluster apply cluster.tfplan
terraform -chdir=common/terraform/confluent-cluster plan -detailed-exitcode
```

An unchanged second plan exits 0. Account and topic provisioning in
`../confluent` reads this root's state outputs, so there is no cluster ID to copy
into its source. Run that workflow after this one. Keep both gitignored state
files; do not delete state to rerun provisioning.

Cluster deletion is protected with Terraform `prevent_destroy`. Standard clusters
do not support the Confluent API deletion-protection setting. Deliberate Terraform
teardown requires removing `prevent_destroy` before destroy. This workflow does not start
Compose or create application credentials. After a cluster replacement, update
`KAFKA_BROKER` in `.env` from `bootstrap_endpoint` (remove `SASL_SSL://`) and issue
Kafka credentials for the replacement cluster; keys scoped to a deleted cluster
cannot be reused.
