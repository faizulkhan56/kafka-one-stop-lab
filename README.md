# Kafka One-Stop Lab

A single project that combines:

1. Kafka introduction and architecture
2. Producer/message/broker/topic/partition/consumer theory
3. Consumer groups and offsets
4. Node.js + KafkaJS implementation
5. Three-node KRaft lab
6. Consumer-group experiments
7. Broker/consumer failure testing
8. Automated integration testing
9. Gradual production deployment guidance
10. A ready-to-edit Medium article

> The earlier source labs used ZooKeeper. This project modernizes the implementation to KRaft while preserving the same learning concepts.

## Architecture

```text
Node.js Producer
       |
       v
+----------------------------------+
| 3-node Kafka KRaft lab cluster   |
|                                  |
| riders_update                    |
|   P0 -> north                    |
|   P1 -> central                  |
|   P2 -> south                    |
|                                  |
| RF=3, minISR=2                   |
+----------------------------------+
        |                  |
        v                  v
 Consumer Group 1     Consumer Group 2
 consumer-a           analytics-a
 consumer-b
 consumer-c

Within a group: partitions are shared.
Across groups: the stream is consumed independently.
```

## Project structure

```text
generated_kafka_one_stop_lab/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── package.json
├── README.md
├── src/
│   ├── admin.js
│   ├── client.js
│   ├── config.js
│   ├── consumer.js
│   └── producer.js
├── test/
│   └── kafka.integration.test.js
└── docs/
    ├── MEDIUM_ARTICLE.md
    ├── PRODUCTION_DEPLOYMENT.md
    └── TESTING_GUIDE.md
```

## Core mental model

### Producer

Writes events to Kafka.

### Message/record

```text
Headers -> metadata
Key     -> identity/routing/ordering aid
Value   -> payload
```

### Broker

A Kafka server that stores partitions and handles read/write requests.

### Topic

A named stream of events.

### Partition

A topic subdivision used for parallelism and scalability. Ordering is within a partition.

### Offset

A record's sequential position inside a partition.

### Consumer

Reads records.

### Consumer group

Consumers sharing the same `group.id` cooperate.

With 3 partitions:

```text
1 consumer  -> 3 partitions
2 consumers -> split 3 partitions
3 consumers -> one partition each
4 consumers -> one consumer idle
```

Different group IDs consume independently.

## Prerequisites

- Docker Engine / Docker Desktop + Compose v2
- Node.js 20+
- npm

## Quick start

```bash
docker compose up -d
docker compose ps

cp .env.example .env
npm install
npm run check
npm run topic:create
```

### Consumer group 1

Terminal 1:

```bash
node src/consumer.js group1 consumer-a
```

Terminal 2:

```bash
node src/consumer.js group1 consumer-b
```

### Independent group 2

Terminal 3:

```bash
node src/consumer.js group2 analytics-a
```

### Producer

Terminal 4:

```bash
npm run producer
```

Input:

```text
alice north
bob central
carol south
```

Teaching routing:

```text
north   -> P0
central -> P1
south   -> P2
```

Or generate a batch:

```bash
npm run producer:demo
```

## Create a third consumer in group1

```bash
node src/consumer.js group1 consumer-c
```

Kafka rebalances. Three partitions can now be processed by three group members.

A fourth same-group consumer will be idle because there are only three partitions.

## Inspect consumer-group offsets/lag

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --group group1
```

## Automated integration test

```bash
npm run test:integration
```

This proves that two different consumer groups each receive the full test stream independently.

## Broker-failure experiment

With RF=3, minISR=2 and producer acks=all:

```bash
docker stop kafka-3
npm run producer:demo
```

One broker can fail while two in-sync replicas remain.

Then:

```bash
docker stop kafka-2
npm run producer:demo
```

Now minISR=2 cannot be satisfied, so durable writes should fail.

Restore:

```bash
docker start kafka-2 kafka-3
```

## Clean reset

```bash
docker compose down -v
```

## Detailed documents

- `docs/TESTING_GUIDE.md` — step-by-step experiments
- `docs/PRODUCTION_DEPLOYMENT.md` — gradual production-grade design and final rollout
- `docs/MEDIUM_ARTICLE.md` — long-form blog/handout draft

## Important production note

The local Compose environment is a learning environment. It deliberately uses plaintext listeners and combined broker/controller roles. Do not deploy it unchanged to production.

For production, the guide progresses toward:

```text
3 dedicated KRaft controllers
3+ dedicated brokers
TLS
authentication
ACLs
private networking
persistent storage
RF=3 / minISR=2 pattern for critical topics
acks=all
idempotent producer
consumer lag monitoring
failure testing
DR
staged production cutover
```

## Why this project combines the three labs well

It keeps the original theory:

```text
producer -> message -> topic -> partition -> broker -> consumer
```

adds the consumer-group rule:

```text
same group -> share partitions
different groups -> independent consumption
```

and makes it executable in Node.js with KafkaJS, topic administration, real offsets, rebalancing, integration tests and broker-failure experiments.
