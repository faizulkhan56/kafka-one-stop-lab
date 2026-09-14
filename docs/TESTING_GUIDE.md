# Kafka One-Stop Lab — Testing Guide

## Prerequisites

- Docker Engine / Docker Desktop with Compose v2
- Node.js 20+
- npm
- free host ports 29092, 39092 and 49092

## 1. Start Kafka

```bash
docker compose up -d
docker compose ps
```

## 2. Install dependencies and validate JavaScript

```bash
cp .env.example .env
npm install
npm run check
```

## 3. Create the topic

```bash
npm run topic:create
```

Expected design:

```text
topic=riders_update
partitions=3
replication.factor=3
min.insync.replicas=2
```

## 4. Partition-routing experiment

Consumer:

```bash
node src/consumer.js group1 consumer-a
```

Producer:

```bash
npm run producer
```

Input:

```text
alice north
bob central
carol south
dave north
```

Expected:

```text
north   -> partition 0
central -> partition 1
south   -> partition 2
```

## 5. Consumer-group load-balancing experiment

Start two consumers with the same group ID:

```bash
node src/consumer.js group1 consumer-a
node src/consumer.js group1 consumer-b
```

Then:

```bash
npm run producer:demo
```

With 3 partitions and 2 consumers, Kafka assigns the 3 partitions across those 2 members.

Add a third:

```bash
node src/consumer.js group1 consumer-c
```

After rebalance, 3 consumers can each own one partition.

Add a fourth:

```bash
node src/consumer.js group1 consumer-d
```

One member is idle because there are only 3 partitions.

## 6. Independent consumer-group experiment

Start:

```bash
node src/consumer.js group1 dispatch-a
node src/consumer.js group2 analytics-a
```

Produce new messages.

Both groups receive the stream independently. Members *inside* one group share work; different groups keep separate offsets.

## 7. Inspect groups and lag

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 \
  --list
```

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --group group1
```

Important fields:

- CURRENT-OFFSET
- LOG-END-OFFSET
- LAG
- CONSUMER-ID
- CLIENT-ID

Conceptually:

```text
lag = log-end offset - current offset
```

## 8. Rebalance/failure experiment

Run several members of `group1`, then stop one with Ctrl+C.

Observe that Kafka reassigns the released partition(s) to remaining group members.

## 9. Broker-failure durability experiment

With RF=3, minISR=2 and producer acks=all:

```bash
docker stop kafka-3
npm run producer:demo
```

Writes can continue if two in-sync replicas remain.

Then:

```bash
docker stop kafka-2
npm run producer:demo
```

Now the minimum ISR cannot be satisfied, so writes should fail rather than be acknowledged with insufficient durability.

Restore:

```bash
docker start kafka-2 kafka-3
```

## 10. Automated integration test

With all brokers running:

```bash
npm run test:integration
```

The test creates a temporary 3-partition topic, starts two different consumer groups, publishes 9 records and verifies that each group independently receives all 9.

## 11. Clean reset

```bash
docker compose down -v
```
