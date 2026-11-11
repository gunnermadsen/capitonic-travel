# Kafka topic creation

A one-shot TypeScript job. `src/main.ts` reads the required topics from
`common/configs/kafka/topics/manifest.json`, connects to Kafka, calls KafkaJS
`createTopics`, disconnects and exits. Existing topics are skipped by Kafka;
failures exit with status 1. It does not alter or delete existing topics, reconcile
configuration, or manage ACLs.

The definitions create `flight.updates`, `itinerary.events`,
`connection.assessments`, and `ingestion.rejections`, each with three partitions,
one replica and seven-day delete retention. Other manifest fields describe
contracts and infrastructure permissions; the creation job does not process them.

```sh
npm run build
docker compose -f docker-compose.local.yml run --rm kafka-topics
```

The job uses the existing topic-manager certificate with CA and hostname
verification. Defaults are `kafka:9092` and `/run/identity`; optional overrides are
`KAFKA_BROKER`, `IDENTITY_DIR`, and `TOPIC_MANIFEST`. Connection retries are bounded,
and the process has a 120-second deadline.

Kafka permissions are an infrastructure prerequisite. Existing ACLs remain intact.
For a fresh broker, use the existing `kafka-tools` service and vendor
`kafka-acls.sh` to provision the topic-manager's Create/Describe permissions on the
required topics and workload permissions declared in the manifest. The creation
job does not bootstrap identities or workload authorization.

This job is included only in `docker-compose.local.yml`. The Confluent deployment
excludes it entirely: cloud topics and permissions belong to Terraform and must
exist before starting the ingester. See the
[Terraform configuration](../../common/terraform/confluent/README.md).
