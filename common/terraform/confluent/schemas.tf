# Register each topic's value contract directly from the authoritative package.
locals {
  contracts = jsondecode(file("${path.module}/../../../packages/contracts/src/schemas/events.v1.schema.json"))
  shared_definitions = {
    for name, definition in local.contracts["$defs"] : name => definition
    if !contains([for topic in local.topics : topic.contract.definition], name)
  }
}

data "confluent_schema_registry_cluster" "existing" {
  provider = confluent.accounts
  environment {
    id = data.terraform_remote_state.cluster.outputs.environment_id
  }
}

resource "confluent_role_binding" "schema_owner" {
  provider = confluent.accounts
  for_each = local.topics

  principal   = "User:${confluent_service_account.provisioner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.existing.resource_name}/subject=${each.key}-value"
}

resource "confluent_api_key" "schema_registry" {
  provider     = confluent.accounts
  display_name = "capitonic-terraform-schemas"
  description  = "Provision only the four Capatonic value schema subjects."
  owner {
    id          = confluent_service_account.provisioner.id
    api_version = confluent_service_account.provisioner.api_version
    kind        = confluent_service_account.provisioner.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.existing.id
    api_version = data.confluent_schema_registry_cluster.existing.api_version
    kind        = data.confluent_schema_registry_cluster.existing.kind
    environment {
      id = data.terraform_remote_state.cluster.outputs.environment_id
    }
  }
  depends_on = [confluent_role_binding.schema_owner]
}

provider "confluent" {
  alias                         = "schemas"
  schema_registry_id            = data.confluent_schema_registry_cluster.existing.id
  schema_registry_rest_endpoint = data.confluent_schema_registry_cluster.existing.rest_endpoint
  schema_registry_api_key       = confluent_api_key.schema_registry.id
  schema_registry_api_secret    = confluent_api_key.schema_registry.secret
}

resource "confluent_schema" "topic_values" {
  provider = confluent.schemas
  for_each = local.topics

  subject_name = "${each.key}-value"
  format       = "JSON"
  schema = jsonencode(merge(local.contracts["$defs"][each.value.contract.definition], {
    "$schema" = local.contracts["$schema"]
    "$id"     = "${local.contracts["$id"]}/${each.value.contract.definition}"
    "$defs"   = local.shared_definitions
  }))
  # Immutable tracked versions prevent silently losing older versions from state.
  recreate_on_update = true
  hard_delete        = true
  lifecycle {
    prevent_destroy = true
    precondition {
      condition     = each.value.contract.version == local.contracts["$defs"].envelope.properties.schema_version.const
      error_message = "Topic contract version must match packages/contracts."
    }
  }
}

output "KAFKA_FLIGHT_SCHEMA_ID" {
  value = confluent_schema.topic_values["flight.updates"].schema_identifier
}

output "KAFKA_REJECTION_SCHEMA_ID" {
  value = confluent_schema.topic_values["ingestion.rejections"].schema_identifier
}
