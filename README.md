# Kafka Cluster Linking DR — Istanbul ⇄ Ankara

Spins up two independent Kafka clusters (Istanbul, Ankara), each with an isolated KRaft topology (3 controllers + 3 brokers), on Docker Compose. The goal is to provide a sandbox for testing a cross-region BIDIRECTIONAL mirror + DR failover architecture with Confluent Cluster Linking.

## Topology

```
ISTANBUL (CLUSTER_ID: XZp5Eb8audug4d4_3Czp2g)       ANKARA (CLUSTER_ID: mtAYBSOIZT28ihb8XJdANw)
  3x controller (node.id 1-3)                        3x controller (node.id 1-3)
  3x broker     (node.id 11-13)                       3x broker     (node.id 11-13)
```

Brokers and controllers run as separate processes (isolated KRaft) — this is required for the coordinator election mechanism that BIDIRECTIONAL cluster links depend on; combined mode (broker+controller in a single process) breaks that mechanism.

The `Dockerfile` extends `confluentinc/cp-server:8.3.0` and bakes the cluster-link config files from `configs/` into the image under `/kafka-configs/` — so you don't need a separate `docker cp` step before creating links.

## Requirements

- Docker + Docker Compose v2

## Bringing Up the Environment

```bash
docker compose up -d
```

The `kafka-dr-node:local` image is built automatically on first run. Wait until all 12 containers are `healthy`:

```bash
docker compose ps
```

## Quickstart — Native Topic + BIDIRECTIONAL Link + Mirror

```bash
# 1) Native "odeme" topic on both clusters
docker exec istanbul-broker-1 kafka-topics --bootstrap-server istanbul-broker-1:9092 --create --topic odeme --partitions 3 --replication-factor 3
docker exec ankara-broker-1 kafka-topics --bootstrap-server ankara-broker-1:9092 --create --topic odeme --partitions 3 --replication-factor 3

# 2) BIDIRECTIONAL link (the link name MUST be identical on both sides —
#    otherwise consumer offset sync silently breaks with ClusterLinkNotFoundException)
docker exec istanbul-broker-1 kafka-cluster-links --bootstrap-server istanbul-broker-1:9092 --create --link ist-ank-link --config-file /kafka-configs/ist-side-link.properties --consumer-group-filters-json-file /kafka-configs/consumer-group-filters.json
docker exec ankara-broker-1 kafka-cluster-links --bootstrap-server ankara-broker-1:9092 --create --link ist-ank-link --config-file /kafka-configs/ank-side-link.properties --consumer-group-filters-json-file /kafka-configs/consumer-group-filters.json

# 3) Mirror topics
docker exec ankara-broker-1 kafka-mirrors --bootstrap-server ankara-broker-1:9092 --create --mirror-topic ist.odeme --source-topic odeme --link ist-ank-link
docker exec istanbul-broker-1 kafka-mirrors --bootstrap-server istanbul-broker-1:9092 --create --mirror-topic ank.odeme --source-topic odeme --link ist-ank-link

# 4) Verify
docker exec ankara-broker-1 kafka-mirrors --bootstrap-server ankara-broker-1:9092 --describe --links ist-ank-link --topics ist.odeme
```

## Tearing Down

```bash
docker compose down -v
```

(`-v` also removes the volumes, for a fully clean restart.)

## Notes

- `configs/ist-side-link.properties` is for the link running on Istanbul: it points at Ankara's brokers and defines the `ank.` prefix.
- `configs/ank-side-link.properties` is for the link running on Ankara: it points at Istanbul's brokers and defines the `ist.` prefix.
- `configs/consumer-group-filters.json` defines the group filter (`*` — all groups) required by `consumer.offset.sync.enable=true`.
