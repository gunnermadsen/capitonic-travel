terraform {
  required_version = ">= 1.14.0, < 2.0.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.94.0"
    }
  }
}

locals {
  # Read only these credentials, under their canonical root .env names.
  # Values are literal, optionally quoted; shell expansion is not performed.
  credentials = {
    for name in [
      "CONFLUENT_CLOUD_API_KEY",
      "CONFLUENT_CLOUD_API_SECRET",
    ] : name => sensitive(trim(regex("(?m)^${name}=([^\\r\\n]*)", file("${path.module}/../../../.env"))[0], " \t\"'"))
  }

  topics = {
    for topic in jsondecode(file("${path.module}/../../configs/kafka/topics/manifest.json")).topics : topic.name => topic
  }

  # Only the current ingester and its read-back verification use this account.
  acls = {
    flight_write       = { type = "TOPIC", name = "flight.updates", operation = "WRITE" }
    flight_describe    = { type = "TOPIC", name = "flight.updates", operation = "DESCRIBE" }
    rejection_write    = { type = "TOPIC", name = "ingestion.rejections", operation = "WRITE" }
    rejection_describe = { type = "TOPIC", name = "ingestion.rejections", operation = "DESCRIBE" }
    idempotent_write   = { type = "CLUSTER", name = "kafka-cluster", operation = "IDEMPOTENT_WRITE" }
    flight_read        = { type = "TOPIC", name = "flight.updates", operation = "READ" }
    verification_group = { type = "GROUP", name = "airline-archive-flights", operation = "READ" }
  }
}

provider "confluent" {
  kafka_id            = data.terraform_remote_state.cluster.outputs.cluster_id
  kafka_rest_endpoint = data.terraform_remote_state.cluster.outputs.rest_endpoint
  kafka_api_key       = confluent_api_key.provisioner.id
  kafka_api_secret    = confluent_api_key.provisioner.secret
  cloud_api_key       = local.credentials.CONFLUENT_CLOUD_API_KEY
  cloud_api_secret    = local.credentials.CONFLUENT_CLOUD_API_SECRET
}

resource "confluent_kafka_topic" "topics" {
  for_each = local.topics

  topic_name       = each.key
  partitions_count = each.value.partitions
  # Cloud manages replication and broker-specific settings.
  config = {
    for name in ["cleanup.policy", "retention.ms", "retention.bytes"] : name => each.value.config[name]
  }

  lifecycle {
    prevent_destroy = true
    precondition {
      condition     = alltrue([for value in local.credentials : length(value) > 0])
      error_message = "Fill CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET in the root .env before provisioning."
    }
  }
}

resource "confluent_kafka_acl" "ingester" {
  for_each = local.acls

  resource_type = each.value.type
  resource_name = each.value.name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.ingester.id}"
  host          = "*"
  operation     = each.value.operation
  permission    = "ALLOW"

  depends_on = [confluent_kafka_topic.topics]
}
