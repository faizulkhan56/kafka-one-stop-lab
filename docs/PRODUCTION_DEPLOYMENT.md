# Production-Grade Apache Kafka — Gradual Deployment Guide

This document starts where the lab ends. The local Compose environment is intentionally convenient, plaintext, and uses combined broker/controller nodes. **Do not deploy it unchanged to production.**

## Stage 1 — Requirements

Before creating servers, document:

- events/sec and peak events/sec;
- average and maximum event size;
- retention period;
- replay requirements;
- topic count and partition estimate;
- producer count;
- consumer groups;
- latency SLO;
- RPO/RTO;
- security/compliance requirements;
- expected yearly growth.

Basic ingress estimate:

```text
bytes/sec = events/sec × average event size
```

Replication and consumer reads increase actual disk/network work.

## Stage 2 — Managed vs self-managed

A managed Kafka service reduces operational ownership. A self-managed cluster means your team owns KRaft quorum, brokers, disk, network, TLS, authentication, ACLs, upgrades, monitoring, incidents and DR.

Make this decision before designing nodes.

## Stage 3 — Production topology

Lab:

```text
kafka-1 = broker + controller
kafka-2 = broker + controller
kafka-3 = broker + controller
```

Production starting pattern:

```text
3 dedicated KRaft controllers
3 or more dedicated Kafka brokers
```

Dedicated roles let controllers and brokers be scaled, protected and rolled independently.

Place replicas across intended failure domains.

## Stage 4 — Storage and hosts

Plan:

- CPU;
- memory/JVM;
- page cache;
- persistent disk type;
- IOPS;
- disk throughput;
- filesystem;
- network bandwidth;
- file descriptors;
- free-space headroom.

Kafka capacity is strongly storage/network dependent.

## Stage 5 — KRaft

Each node needs a unique `node.id`.

Controller concept:

```properties
process.roles=controller
node.id=1
controller.listener.names=CONTROLLER
controller.quorum.voters=1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
listeners=CONTROLLER://controller-1:9093
```

Broker concept:

```properties
process.roles=broker
node.id=101
controller.listener.names=CONTROLLER
controller.quorum.voters=1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
listeners=INTERNAL://:9092
advertised.listeners=INTERNAL://broker-1.kafka.internal:9092
inter.broker.listener.name=INTERNAL
```

## Stage 6 — Network and advertised listeners

A Kafka client first contacts a bootstrap broker, receives metadata, and then connects to broker addresses returned in that metadata.

Therefore:

```text
bootstrap reachable != whole cluster reachable
```

All advertised broker addresses must resolve and be reachable from the application network.

Prefer private networks. Restrict ports by firewall/security group. Avoid unnecessary public listeners.

## Stage 7 — TLS

Replace the lab's PLAINTEXT communication with TLS according to your security design.

Plan:

- CA;
- broker certificates;
- hostname verification;
- client trust;
- secret storage;
- certificate rotation.

Never commit private keys/passwords to Git.

## Stage 8 — Authentication

Choose an appropriate Kafka-supported mechanism, such as:

- mTLS;
- SASL/SCRAM;
- SASL/OAUTHBEARER where suitable.

Use distinct service identities.

## Stage 9 — Authorization

Apply least-privilege ACLs.

Example intent:

```text
rider-api         -> WRITE riders_update
dispatch-service  -> READ riders_update + GROUP dispatch-service
analytics-service -> READ riders_update + GROUP analytics-service
```

Keep admin privileges out of normal applications.

Disable uncontrolled topic creation:

```properties
auto.create.topics.enable=false
```

## Stage 10 — Durability

Common critical-topic starting pattern:

```text
replication.factor=3
min.insync.replicas=2
producer acks=all
```

Review internal topics too:

```properties
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

Topic overrides can differ from broker defaults.

## Stage 11 — Topic design

For every topic define:

- owner;
- purpose;
- partition count;
- replication factor;
- minISR;
- retention;
- cleanup policy;
- event key;
- schema/contract;
- producers;
- consumer groups;
- SLO.

Too few partitions limit parallelism. Too many add metadata and operational overhead.

## Stage 12 — Keys and ordering

Use a stable key when entity-level ordering matters, e.g.:

```text
key=riderId
```

Kafka ordering is per partition, not global across the entire topic.

## Stage 13 — Producer configuration

For critical streams start from:

```text
acks=all
idempotence enabled
retries enabled
bounded timeouts
error metrics/logging
```

Evaluate batching and compression using measurements.

Failed publishes must be surfaced to the application.

## Stage 14 — Consumer reliability

Define:

- stable group ID;
- offset/commit strategy;
- graceful shutdown;
- retry behavior;
- idempotent business processing;
- poison-message handling;
- dead-letter strategy where appropriate;
- processing timeout;
- lag SLO.

At-least-once delivery can produce duplicates, so external side effects need a deliberate idempotency strategy.

## Stage 15 — Schema governance

JSON is convenient in the lab; production event contracts need versioning and compatibility rules. Larger platforms commonly use a schema registry.

## Stage 16 — Observability

Monitor cluster:

- broker availability;
- controller/quorum health;
- offline partitions;
- under-replicated partitions;
- ISR changes;
- produce/fetch latency;
- error rate;
- disk capacity and latency;
- network;
- JVM/GC;
- file descriptors.

Monitor consumer groups:

- current offset;
- log-end offset;
- lag;
- lag growth;
- member count;
- rebalance frequency.

Monitor applications:

- publish success/failure;
- processing success/failure;
- retries;
- dead-letter count;
- end-to-end event latency.

## Stage 17 — Capacity/load test

Before production:

1. use realistic record size;
2. run normal throughput;
3. run peak throughput;
4. sustain peak;
5. remove one broker;
6. restart consumers;
7. measure lag/catch-up time;
8. watch disk/network saturation;
9. test alert thresholds.

## Stage 18 — Failure testing

Prove:

- broker loss;
- consumer loss/rebalance;
- controller loss while quorum majority remains;
- network interruption;
- disk-pressure alerting;
- certificate-rotation procedure.

## Stage 19 — DR

Replication inside one cluster is high availability, not automatically disaster recovery.

Define:

- disaster boundary;
- secondary cluster requirement;
- cross-cluster replication strategy;
- RPO;
- RTO;
- configuration/ACL recovery;
- failover and failback runbooks.

Test them.

## Stage 20 — CI/CD and IaC

Store approved non-secret configuration in version control/IaC.

Suggested flow:

```text
validate -> dev -> integration/load -> staging -> approval -> prod
```

Secrets come from a secret manager.

## Stage 21 — Staging

Keep staging behaviorally similar to production:

- same Kafka version family;
- same TLS/auth pattern;
- same ACL model;
- same topic-policy pattern;
- same application configuration structure.

## Stage 22 — Production readiness gate

- [ ] 3-controller quorum healthy
- [ ] required broker count healthy
- [ ] failure-domain distribution verified
- [ ] storage latency/capacity validated
- [ ] TLS enabled
- [ ] authentication enabled
- [ ] ACLs verified
- [ ] no unnecessary public listeners
- [ ] advertised listeners tested from app network
- [ ] topic RF/minISR verified
- [ ] internal-topic replication verified
- [ ] auto topic creation disabled
- [ ] critical producers use acks=all
- [ ] idempotence/retry behavior tested
- [ ] consumer-group IDs reviewed
- [ ] lag dashboard live
- [ ] cluster dashboard live
- [ ] alerts tested
- [ ] schema compatibility policy approved
- [ ] load test passed
- [ ] broker-failure test passed
- [ ] consumer-failure test passed
- [ ] DR plan approved/tested
- [ ] rollback/cutback plan prepared
- [ ] runbooks/on-call ownership ready

## Stage 23 — Final production deployment

1. Deploy the KRaft controller quorum.
2. Verify controller majority/health.
3. Deploy brokers with persistent disks and production listeners.
4. Validate TLS from an application network.
5. Validate authentication with a non-admin service identity.
6. Create production topics with reviewed partitions/RF/minISR/retention.
7. Apply ACLs.
8. Verify leaders, replicas and ISR.
9. Deploy consumers.
10. Deploy producers.
11. Start with low/controlled traffic where architecture permits.
12. Watch produce errors, request latency, ISR, under-replicated partitions, lag and rebalances.
13. Ramp traffic gradually.
14. Confirm throughput/latency/lag/error SLOs.
15. Record final versions, configs, evidence, runbooks and ownership.

## Anti-patterns

Do not:

- copy the plaintext lab Compose to production;
- run RF=1 for critical data;
- expose brokers openly to the internet;
- share one admin credential across apps;
- advertise `localhost` to remote clients;
- ignore lag;
- assume replication equals DR;
- let applications create arbitrary topics;
- change partition counts casually for keyed streams;
- treat exactly-once as magic for external database/API side effects.
