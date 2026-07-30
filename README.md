# Kafka Cluster Linking DR — İstanbul ⇄ Ankara

İki bağımsız Kafka cluster'ını (İstanbul, Ankara), her biri izole KRaft (3 controller + 3 broker) topolojisiyle, Docker Compose üzerinde ayağa kaldırır. Amaç, Confluent Cluster Linking ile bölgeler arası BIDIRECTIONAL mirror + DR failover mimarisini test edebileceğin bir sandbox sağlamak.

## Topoloji

```
İSTANBUL (CLUSTER_ID: XZp5Eb8audug4d4_3Czp2g)      ANKARA (CLUSTER_ID: mtAYBSOIZT28ihb8XJdANw)
  3x controller (node.id 1-3)                        3x controller (node.id 1-3)
  3x broker     (node.id 11-13)                       3x broker     (node.id 11-13)
```

Broker ve controller'lar ayrı process olarak çalışır (izole KRaft) — bu, BIDIRECTIONAL cluster link'lerin ihtiyaç duyduğu coordinator seçim mekanizması için gereklidir; combined mode (broker+controller aynı process) bu mekanizmayı tıkar.

`Dockerfile`, `confluentinc/cp-server:8.3.0` imajını temel alıp `configs/` altındaki cluster-link konfigürasyon dosyalarını imajın içine `/kafka-configs/` altına gömer — böylece link oluşturma komutları için ayrıca `docker cp` yapmana gerek kalmaz.

## Gereksinimler

- Docker + Docker Compose v2

## Ortamı Ayağa Kaldırma

```bash
docker compose up -d
```

İlk çalıştırmada `kafka-dr-node:local` imajı otomatik build edilir. 12 container'ın da `healthy` olmasını bekle:

```bash
docker compose ps
```

## Hızlı Başlangıç — Native Topic + BIDIRECTIONAL Link + Mirror

```bash
# 1) Her iki cluster'da native "odeme" topic'i
docker exec istanbul-broker-1 kafka-topics --bootstrap-server istanbul-broker-1:9092 --create --topic odeme --partitions 3 --replication-factor 3
docker exec ankara-broker-1 kafka-topics --bootstrap-server ankara-broker-1:9092 --create --topic odeme --partitions 3 --replication-factor 3

# 2) BIDIRECTIONAL link (iki tarafta da AYNI link ismi zorunlu — aksi halde
#    consumer offset sync ClusterLinkNotFoundException ile sessizce bozulur)
docker exec istanbul-broker-1 kafka-cluster-links --bootstrap-server istanbul-broker-1:9092 --create --link ist-ank-link --config-file /kafka-configs/ist-side-link.properties --consumer-group-filters-json-file /kafka-configs/consumer-group-filters.json
docker exec ankara-broker-1 kafka-cluster-links --bootstrap-server ankara-broker-1:9092 --create --link ist-ank-link --config-file /kafka-configs/ank-side-link.properties --consumer-group-filters-json-file /kafka-configs/consumer-group-filters.json

# 3) Mirror topic'ler
docker exec ankara-broker-1 kafka-mirrors --bootstrap-server ankara-broker-1:9092 --create --mirror-topic ist.odeme --source-topic odeme --link ist-ank-link
docker exec istanbul-broker-1 kafka-mirrors --bootstrap-server istanbul-broker-1:9092 --create --mirror-topic ank.odeme --source-topic odeme --link ist-ank-link

# 4) Doğrulama
docker exec ankara-broker-1 kafka-mirrors --bootstrap-server ankara-broker-1:9092 --describe --links ist-ank-link --topics ist.odeme
```

## Ortamı Kapatma

```bash
docker compose down -v
```

(`-v`, volume'leri de siler — tamamen sıfırdan başlamak için.)

## Notlar

- `configs/ist-side-link.properties` İstanbul'da çalışan link için, Ankara'nın broker adreslerini ve `ank.` prefix'ini tanımlar.
- `configs/ank-side-link.properties` Ankara'da çalışan link için, İstanbul'un broker adreslerini ve `ist.` prefix'ini tanımlar.
- `configs/consumer-group-filters.json`, `consumer.offset.sync.enable=true` için zorunlu olan grup filtresini (`*` — tüm gruplar) tanımlar.
