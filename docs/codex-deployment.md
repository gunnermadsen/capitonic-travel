# Ingester in Codex

Use Compose as the deployment entrypoint from the repository root:

```sh
docker compose -f docker-compose.codex.yml up -d --build
docker compose -f docker-compose.codex.yml ps
docker compose -f docker-compose.codex.yml logs --tail=50 ingester
```

This entrypoint builds contracts and the ingester with Node 22.21.1 inside Docker,
uses the host's native architecture, and runs the ingester plus a one-time state-volume permission initializer. No host npm install,
compiled files, or `.env` file is required. Docker supplies its configured proxy
settings; Compose mounts the Codex proxy CA for both building and running the image.
The ingester retains checkpoints in `ingester-state`, takes a process lock, reports
health, and restarts if it exits while the environment is running.

Compose reads credentials from the Codex environment and deployment settings
from `docker-compose.confluent.yml`:

| Variable | Source |
| --- | --- |
| `FLIGHTAWARE_API_KEY` | Codex secret scoped to `aeroapi.flightaware.com` |
| `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET` | Kafka application credentials |
| `KAFKA_BROKER` | Cluster bootstrap hostname and port in Compose |
| `KAFKA_FLIGHT_SCHEMA_ID`, `KAFKA_REJECTION_SCHEMA_ID` | Registered schema IDs in Compose |
| `CODEX_PROXY_CERT` | Supplied by Codex |

Compose passes application secrets through service environment variables at runtime;
they are not build arguments or image contents. Do not save expanded `docker compose
config` output, which can contain credentials. Non-sensitive settings remain in `docker-compose.confluent.yml`, including the
broker and schema IDs. The default source is AeroAPI in
PUBLIC mode, airport KJFK, polling every five minutes. Edit the Compose settings to change these defaults. For Firehose, select `INGESTER_PRODUCT=flight-updates`
and set `FIREHOSE_USERNAME` in Compose and `FIREHOSE_API_KEY` in the environment.

The environment must support authenticated Kafka TCP connections, including broker
addresses returned in Kafka metadata. HTTP domain rules alone do not provide this.
The current Kafka and Firehose clients use direct TLS sockets; they do not implement
the Codex TCP CONNECT proxy. Codex domain-scoped secret substitution on HTTPS does
not establish that these credentials work in raw TCP SASL or Firehose authentication.
Resolve and validate that access before treating this deployment as ready. Firehose
also requires access to port 1501.

The Kafka topics `flight.updates` and `ingestion.rejections`, schema IDs, and
application permissions must already exist. The ingester does not need Cloud
management credentials at runtime. Publishing an environment does not execute
Compose; use the command above when starting the deployment in a running environment.
