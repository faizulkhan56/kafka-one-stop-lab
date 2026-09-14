# Apache Kafka From Zero to Production: A One-Stop Theory, Consumer-Group and Node.js Hands-On Guide

This article combines Kafka theory, a runnable Node.js/KafkaJS project, consumer-group experiments, failure testing and a gradual production deployment roadmap.

## 1. The mental model

Kafka is a distributed event-streaming platform.

```text
Producer
   |
   v
+----------------------+
| Kafka Cluster        |
| Topic                |
|  |- Partition 0      |
|  |- Partition 1      |
|  `- Partition 2      |
+----------------------+
   |
   v
Consumer Groups
```

A producer appends records to a topic. Kafka stores the records in ordered partitions. Consumers read them, and consumer groups track progress with offsets.

Kafka is not merely "a queue"; it is a durable distributed event log that can serve many independent downstream applications.

## 2. Core concepts

### Producer

Writes events.

### Record

Think:

```text
Headers -> metadata
Key     -> identity/partitioning/ordering aid
Value   -> payload
```

### Broker

A Kafka server. Brokers store partitions, handle reads/writes and replicate data.

### Topic

A named event stream such as:

```text
riders_update
orders_created
payment_completed
```

### Partition

A topic is split into partitions for parallelism and scalability.

Kafka guarantees ordering within a partition, not one global order across all partitions.

### Offset

A sequential position inside a partition.

```text
P0: offset 0, 1, 2, 3...
```

Consumer groups commit offsets to represent progress.

### Consumer

Reads records and performs business work.

### Consumer group

Consumers with the same `group.id` cooperate.

Critical rule:

> One partition is assigned to at most one consumer within a consumer group at a time.

With 3 partitions:

```text
1 consumer  -> handles 3 partitions
2 consumers -> partitions split 2 + 1
3 consumers -> 1 partition each
4 consumers -> 1 consumer idle
```

Different consumer groups, however, consume independently:

```text
riders_update
   |------> dispatch group
   |------> analytics group
   `------> audit group
```

Each group maintains separate progress.

## 3. Rebalancing

When a consumer joins, leaves or fails, Kafka redistributes partitions among the remaining members. That is a rebalance.

This gives horizontal scaling and fault tolerance, but production consumers should support graceful shutdown and tolerate reprocessing.

## 4. KRaft vs ZooKeeper

Older learning labs commonly use ZooKeeper. Modern Kafka uses KRaft for metadata management, so this project uses no ZooKeeper container.

The producer/topic/partition/consumer/offset concepts remain the same.

## 5. Our hands-on scenario

We produce rider-location events:

```text
alice north
bob central
carol south
```

For teaching, we explicitly map:

```text
north   -> partition 0
central -> partition 1
south   -> partition 2
```

The topic has:

```text
partitions=3
replication.factor=3
min.insync.replicas=2
```

and the producer uses `acks=all` plus idempotent mode.

## 6. Project structure

```text
kafka-one-stop-lab/
├── docker-compose.yml
├── package.json
├── .env.example
├── README.md
├── src/
│   ├── config.js
│   ├── client.js
│   ├── admin.js
│   ├── producer.js
│   └── consumer.js
├── test/
│   └── kafka.integration.test.js
└── docs/
    ├── TESTING_GUIDE.md
    ├── PRODUCTION_DEPLOYMENT.md
    └── MEDIUM_ARTICLE.md
```

## 7. Start the lab

```bash
docker compose up -d
docker compose ps
cp .env.example .env
npm install
npm run check
npm run topic:create
```

Bootstrap endpoints:

```text
localhost:29092
localhost:39092
localhost:49092
```

## 8. Start consumers

Terminal 1:

```bash
node src/consumer.js group1 consumer-a
```

Terminal 2:

```bash
node src/consumer.js group1 consumer-b
```

Terminal 3, independent group:

```bash
node src/consumer.js group2 analytics-a
```

## 9. Start producer

```bash
npm run producer
```

Enter:

```text
alice north
bob central
carol south
```

The output lets you inspect consumer, group, topic, partition, offset, key, headers and value.

## 10. Prove consumer parallelism

Start a third member of `group1`:

```bash
node src/consumer.js group1 consumer-c
```

Three consumers can now own three partitions.

Start a fourth:

```bash
node src/consumer.js group1 consumer-d
```

One must be idle. This is why partition count limits useful consumer parallelism inside one group.

## 11. Prove independent consumer groups

Keep `group1` and `group2` running and publish new events.

Both groups see the stream independently because their committed offsets are separate.

## 12. Inspect lag

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --group group1
```

Watch:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

Conceptually:

```text
lag = log-end offset - current offset
```

Growing lag means consumers are falling behind.

## 13. Consumer-failure test

Run multiple `group1` members. Stop one with Ctrl+C.

Kafka rebalances and assigns its partitions to the remaining members.

## 14. Broker-failure test

The topic uses RF=3 and minISR=2; producer sends with acks=all.

Stop one broker:

```bash
docker stop kafka-3
npm run producer:demo
```

Writes can continue while two in-sync replicas remain.

Stop a second:

```bash
docker stop kafka-2
npm run producer:demo
```

Now Kafka should refuse durable writes requiring minISR=2. That explicit failure is safer than acknowledging insufficiently replicated critical data.

Restore:

```bash
docker start kafka-2 kafka-3
```

## 15. Automated test

```bash
npm run test:integration
```

It creates a temporary 3-partition topic, starts two different groups, publishes 9 records, and verifies that each group receives all 9.

## 16. Delivery semantics

### At-most-once

Commit before processing. A crash after commit can lose application processing.

### At-least-once

Process then commit. A crash after the side effect but before commit can cause duplicate processing.

Hence production consumers often need idempotency.

### Exactly-once

Kafka transactions can coordinate Kafka work, but external DB/API side effects still require an end-to-end transactional/idempotent design. Exactly-once is not a universal checkbox.

# 17. From laptop lab to production

Do not copy the lab Compose file into production. Move gradually.

## Stage 1 — Requirements

Record:

```text
events/sec
peak
event size
retention
replay window
consumer groups
latency SLO
RPO/RTO
security/compliance
```

## Stage 2 — Managed vs self-managed

Decide whether your team truly wants to own Kafka control plane, storage, TLS, ACLs, upgrades, monitoring and DR.

## Stage 3 — Topology

Lab:

```text
3 combined broker/controller nodes
```

Production starting pattern:

```text
3 dedicated KRaft controllers
3+ dedicated brokers
```

Use failure-domain separation.

## Stage 4 — Storage

Plan IOPS, throughput, latency, retention capacity, free-space margin and broker replacement.

## Stage 5 — KRaft

Unique node IDs, stable controller quorum voters, and separate controller/broker listener design.

## Stage 6 — Network and DNS

All `advertised.listeners` returned to clients must be resolvable and reachable. A reachable bootstrap address alone is not enough.

Prefer private networking and restricted firewall/security-group rules.

## Stage 7 — TLS

Encrypt client-to-broker and appropriate inter-broker traffic. Manage certificates and rotation outside Git.

## Stage 8 — Authentication

Use mTLS, SASL/SCRAM, or suitable OAuth-based authentication according to your platform.

Use distinct application identities.

## Stage 9 — ACL authorization

Grant only required topic and group permissions.

Disable uncontrolled topic creation:

```properties
auto.create.topics.enable=false
```

## Stage 10 — Durability

A common critical-topic pattern:

```text
RF=3
minISR=2
acks=all
```

Also give Kafka internal topics production-appropriate replication.

## Stage 11 — Topic governance

Document owner, partitions, RF, minISR, retention, cleanup policy, event key, schema, producers and consumer groups.

## Stage 12 — Key strategy

Use stable business keys when per-entity ordering matters.

## Stage 13 — Producer reliability

Use durable acknowledgements, idempotence, retry policy, timeouts, metrics and structured errors.

## Stage 14 — Consumer reliability

Define commit strategy, retries, poison-event handling, graceful shutdown, idempotency and lag SLO.

## Stage 15 — Schema governance

Define compatibility/versioning. Larger estates commonly use a schema registry.

## Stage 16 — Monitoring

Cluster:

```text
broker health
controller health
offline/under-replicated partitions
ISR
request latency/errors
disk
network
JVM
```

Consumers:

```text
lag
lag growth
member count
rebalances
```

Apps:

```text
publish/consume success
retries
DLQ
end-to-end latency
```

## Stage 17 — Load test

Use realistic records and sustained peaks. Test one broker down and measure consumer catch-up.

## Stage 18 — Failure test

Prove broker, controller, consumer and network failure behavior before production.

## Stage 19 — DR

One-cluster replication is HA, not automatically regional DR. Define secondary-cluster strategy, RPO, RTO, failover and failback.

## Stage 20 — Staging

Use the same Kafka version/security/configuration pattern as production.

## Stage 21 — Readiness gate

Verify controllers, brokers, storage, TLS, auth, ACLs, advertised listeners, RF/minISR, producer settings, lag dashboards, alerts, failure tests and DR.

## Stage 22 — Final production deployment

A conservative sequence:

```text
1  Deploy KRaft controllers
2  Verify quorum
3  Deploy brokers
4  Verify persistent storage
5  Test TLS
6  Test authentication
7  Apply/test ACLs
8  Create topics
9  Verify leaders/replicas/ISR
10 Deploy consumers
11 Deploy producers
12 Start controlled traffic
13 Observe errors/latency/ISR/lag/rebalances
14 Ramp gradually
15 Confirm SLOs
16 Close change with evidence/runbooks/ownership
```

# 18. Final mental model

At first:

```text
Producer -> Kafka -> Consumer
```

After the lab:

```text
                 KRaft quorum
                     |
Producer -> replicated Kafka partitions
   |                 |
 key/acks             | offsets
   |                  v
   |             consumer groups
   |             rebalance + lag
   |                  |
   +------------------+
        observability

TLS + identity + ACL + persistent storage + failure domains
+ monitoring + load testing + DR + runbooks
= production Kafka platform
```

## Medium publishing checklist

- Add an architecture image after Section 1.
- Add a consumer-group diagram after Section 9.
- Add a screenshot showing two `group1` consumers after Section 10.
- Add consumer-group CLI output after Section 12.
- Add broker-failure output after Section 14.
- Link the GitHub repository near the introduction and conclusion.
- Clearly label the Docker Compose environment as a learning environment, not a production security configuration.
- Use the production stages as the final major section.
- Suggested tags: `Apache Kafka`, `Event-Driven Architecture`, `Node.js`, `DevOps`, `Distributed Systems`.
