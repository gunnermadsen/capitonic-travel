resource "confluent_api_key" "provisioner" {
  provider     = confluent.accounts
  display_name = "capitonic-terraform-kafka"
  description  = "Kafka administration for the Terraform-managed flights cluster."
  owner {
    id          = confluent_service_account.provisioner.id
    api_version = confluent_service_account.provisioner.api_version
    kind        = confluent_service_account.provisioner.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.existing.id
    api_version = data.confluent_kafka_cluster.existing.api_version
    kind        = data.confluent_kafka_cluster.existing.kind
    environment {
      id = data.terraform_remote_state.cluster.outputs.environment_id
    }
  }
  depends_on = [confluent_role_binding.provisioner_cluster_admin]
}

resource "confluent_api_key" "ingester" {
  provider     = confluent.accounts
  display_name = "capitonic-ingester-kafka"
  description  = "Application Kafka access for the flights cluster."
  owner {
    id          = confluent_service_account.ingester.id
    api_version = confluent_service_account.ingester.api_version
    kind        = confluent_service_account.ingester.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.existing.id
    api_version = data.confluent_kafka_cluster.existing.api_version
    kind        = data.confluent_kafka_cluster.existing.kind
    environment {
      id = data.terraform_remote_state.cluster.outputs.environment_id
    }
  }
  depends_on = [confluent_kafka_acl.ingester]
}

output "CONFLUENT_API_KEY" {
  value     = confluent_api_key.ingester.id
  sensitive = true
}

output "CONFLUENT_API_SECRET" {
  value     = confluent_api_key.ingester.secret
  sensitive = true
}

output "KAFKA_BROKER" {
  value = trimprefix(data.confluent_kafka_cluster.existing.bootstrap_endpoint, "SASL_SSL://")
}
