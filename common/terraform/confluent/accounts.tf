# Account bootstrap uses Cloud management credentials only, never the app key.
provider "confluent" {
  alias            = "accounts"
  cloud_api_key    = local.credentials.CONFLUENT_CLOUD_API_KEY
  cloud_api_secret = local.credentials.CONFLUENT_CLOUD_API_SECRET
}

data "terraform_remote_state" "cluster" {
  backend = "local"
  config = {
    path = "${path.module}/../confluent-cluster/terraform.tfstate"
  }
}

data "confluent_kafka_cluster" "existing" {
  provider = confluent.accounts
  id       = data.terraform_remote_state.cluster.outputs.cluster_id
  environment {
    id = data.terraform_remote_state.cluster.outputs.environment_id
  }
}

resource "confluent_service_account" "provisioner" {
  provider     = confluent.accounts
  display_name = "capitonic-terraform"
  description  = "Provisions Capatonic topics and ACLs on the flights cluster."

  lifecycle {
    prevent_destroy = true
    precondition {
      condition = (
        length(local.credentials.CONFLUENT_CLOUD_API_KEY) > 0 &&
        length(local.credentials.CONFLUENT_CLOUD_API_SECRET) > 0
      )
      error_message = "Set CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET in .env to an identity authorized to bootstrap accounts and roles."
    }
  }
}

# Adopt the existing application identity; do not recreate it or rotate its keys.
import {
  provider = confluent.accounts
  to       = confluent_service_account.ingester
  id       = "sa-v1wnjrz"
}

resource "confluent_service_account" "ingester" {
  provider     = confluent.accounts
  display_name = "capitonic-sa"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [description]
  }
}

resource "confluent_role_binding" "provisioner_cluster_admin" {
  provider    = confluent.accounts
  principal   = "User:${confluent_service_account.provisioner.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.existing.rbac_crn
}

output "provisioner_service_account_id" {
  value = confluent_service_account.provisioner.id
}

output "ingester_service_account_id" {
  value = confluent_service_account.ingester.id
}
