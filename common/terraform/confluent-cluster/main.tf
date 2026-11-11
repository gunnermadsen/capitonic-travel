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
  credentials = {
    for name in ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"] :
    name => sensitive(trim(regex("(?m)^${name}=([^\\r\\n]*)", file("${path.module}/../../../.env"))[0], " \t\"'"))
  }
}

provider "confluent" {
  cloud_api_key    = local.credentials.CONFLUENT_CLOUD_API_KEY
  cloud_api_secret = local.credentials.CONFLUENT_CLOUD_API_SECRET
}

resource "confluent_kafka_cluster" "flights" {
  display_name = "capitonic-flights"
  availability = "LOW"
  cloud        = "GCP"
  region       = "us-central1"
  standard {
    max_ecku = 1
  }
  environment {
    id = "env-7137m1"
  }
  lifecycle {
    prevent_destroy = true
  }
}

output "cluster_id" {
  value = confluent_kafka_cluster.flights.id
}
output "environment_id" {
  value = confluent_kafka_cluster.flights.environment[0].id
}
output "rest_endpoint" {
  value = confluent_kafka_cluster.flights.rest_endpoint
}
output "bootstrap_endpoint" {
  value = confluent_kafka_cluster.flights.bootstrap_endpoint
}
