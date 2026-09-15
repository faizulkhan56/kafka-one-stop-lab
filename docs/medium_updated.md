# Apache Kafka From Zero to a Working Node.js Lab — One-Stop Theory, Code, Testing and Production Path

> This guide is designed to be read from top to bottom in one sitting. It connects the Kafka theory directly to the exact Docker Compose and Node.js project used in this lab. The goal is not to memorize Kafka words separately, but to understand how one rider event moves through **producer → topic → partition → broker → consumer group → consumer → offset**, and then prove each concept with commands.

---

## Table of contents

1. The one picture to remember
2. The rider example
3. Kafka terminology in simple language
4. How all concepts connect
5. Project architecture
6. Project folder structure
7. How every file is connected
8. Docker Compose: our Kafka cluster
9. `.env` and application configuration
10. `config.js`
11. `client.js`
12. `admin.js`
13. `producer.js`
14. `consumer.js`
15. `package.json`
16. Start the complete lab
17. Create and inspect the topic
18. Produce rider events
19. Consume rider events
20. Consumer groups and partition sharing
21. Rebalancing experiment
22. Offsets and lag experiment
23. Why an inactive group still appears
24. CLI commands for consumer groups
25. Git Bash path-conversion note
26. Broker replication and failure experiment
27. Automated integration test
28. How the test code works
29. Complete end-to-end request flow
30. Common confusions cleared up
31. From this lab to production Kafka
32. Final cheat sheet

---

# 1. The one picture to remember

Do not begin by memorizing definitions. Remember this flow:

```text
Rider changes location
        |
        v
Node.js PRODUCER
        |
        | sends an event
        v
TOPIC: riders_update
        |
        | event is routed to one partition
        v
PARTITION: P0 / P1 / P2
        |
        | physically stored and replicated by
        v
KAFKA BROKERS
        |
        | read independently by
        v
CONSUMER GROUPS
        |
        | each group's partitions are distributed among
        v
CONSUMERS
        |
        | after processing, the group's progress is remembered as
        v
OFFSET
```

Everything in Kafka fits somewhere in this picture.

---

# 2. The rider example

Our application handles rider-location updates.

Suppose Bob moves to the central region:

```text
bob central
```

The producer turns that into an event approximately like this:

```json
{
  "riderName": "bob",
  "location": "central"
}
```

Our topic is:

```text
riders_update
```

For this learning project, routing is deliberately simple:

```text
north   -> Partition 0
central -> Partition 1
south   -> Partition 2
```

Therefore:

```text
bob central
     |
     v
riders_update
     |
     v
Partition 1
```

This one example will be reused throughout the guide.

---

# 3. Kafka terminology in simple language

## 3.1 Producer

A **producer** is an application that sends events to Kafka.

In this project:

```bash
node src/producer.js
```

The producer does **not** send directly to a consumer. It publishes to a Kafka topic.

```text
Producer -> Kafka Topic
```

## 3.2 Kafka cluster

A **Kafka cluster** is the whole Kafka system made from multiple Kafka servers.

Our lab has three Kafka nodes:

```text
Kafka Cluster
├── kafka-1
├── kafka-2
└── kafka-3
```

## 3.3 Broker

A **broker** is one Kafka server inside the cluster.

A broker receives writes, serves reads, stores partition data, and participates in replication.

```text
Cluster = all Kafka servers together
Broker  = one Kafka server
```

## 3.4 Topic

A **topic** is a named stream/category of events.

Our topic:

```text
riders_update
```

Other possible topics could be:

```text
orders_created
payments_completed
rider_status
```

## 3.5 Partition

A topic is divided into **partitions** so Kafka can scale and process work in parallel.

Our topic:

```text
riders_update
├── Partition 0
├── Partition 1
└── Partition 2
```

Think of partitions as lanes:

```text
P0 -------->
P1 -------->
P2 -------->
```

Kafka guarantees ordering **inside one partition**, not one global order across all partitions.

## 3.6 Message / record

A Kafka record commonly has:

```text
Headers -> metadata
Key     -> identity / routing / ordering aid
Value   -> actual payload
```

For Bob:

```text
Headers:
  event-type = rider-location-updated
  content-type = application/json

Key:
  bob

Value:
  riderName = bob
  location  = central
```

In a real application, a stable `riderId` would usually be a better key than a display name.

## 3.7 Routing

**Routing** means deciding which partition receives a record.

Our training rule is explicitly coded as:

```text
north   -> P0
central -> P1
south   -> P2
```

So Bob's central event goes to P1.

In production, partition routing is commonly based on a stable key such as `riderId` rather than a region hard-coded to a partition.

## 3.8 Offset

An **offset** is the position of a record inside one partition.

Example:

```text
Partition 1

offset 0 -> bob central
offset 1 -> asraf central
offset 2 -> rider-2 central
offset 3 -> rider-5 central
```

Offsets are per partition. P0, P1 and P2 can all have an offset `0`.

A useful mental model:

```text
Partition = lane
Offset    = position inside that lane
```

## 3.9 Consumer

A **consumer** is a running application/process that reads Kafka records.

Example:

```bash
node src/consumer.js group1 consumer-a
```

Here:

```text
group1     = consumer group ID
consumer-a = friendly name used by our program
```

## 3.10 Consumer group

A **consumer group** is a set of consumers cooperating to process a topic.

If the topic has three partitions and one group has three consumers:

```text
P0 -> consumer-a
P1 -> consumer-b
P2 -> consumer-c
```

Important rule:

> Within one consumer group, a partition is assigned to at most one active consumer at a time.

Therefore:

```text
3 partitions + 1 consumer -> one consumer handles all 3
3 partitions + 2 consumers -> one gets 2, one gets 1
3 partitions + 3 consumers -> one partition each
3 partitions + 4 consumers -> one consumer is idle
```

## 3.11 Different consumer groups

Different groups consume the same topic independently.

```text
                 riders_update
                /                            v               v
          group1              group2
         Dispatch            Analytics
```

`group1` does not steal records from `group2`.

A simple rule worth memorizing:

```text
Same group      -> share the work
Different group -> independently consume the stream
```

## 3.12 Consumer-group offset

Kafka remembers each group's progress independently.

Suppose the end of P1 is position 14:

```text
group1 current position = 14
group2 current position = 10
```

Then group1 is caught up, but group2 is behind.

## 3.13 Lag

**Lag** is how far a consumer group is behind the newest records.

Approximately:

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

Example:

```text
CURRENT-OFFSET = 10
LOG-END-OFFSET = 14
LAG            = 4
```

The group is four positions behind.

## 3.14 Rebalancing

A **rebalance** happens when Kafka redistributes partitions among consumers in the same group.

Typical triggers:

- a consumer joins;
- a consumer leaves;
- a consumer crashes;
- the subscription changes;
- partition topology changes.

Example:

```text
Before:
P0,P1,P2 -> consumer-a

consumer-b joins

After rebalance:
P0,P2 -> consumer-a
P1    -> consumer-b
```

## 3.15 Replication

Partitions can have copies on multiple brokers.

With replication factor 3:

```text
Partition 1
├── replica on Broker 1
├── replica on Broker 2
└── replica on Broker 3
```

One replica is the partition leader at a time; the others follow and replicate.

Replication protects against broker failure.

## 3.16 KRaft

Older Kafka labs often use ZooKeeper. This project uses modern **KRaft** metadata management.

For this local lab, each of the three Kafka nodes performs both roles:

```text
kafka-1 = broker + controller
kafka-2 = broker + controller
kafka-3 = broker + controller
```

That is convenient for learning. A serious production design normally separates controller and broker roles.

---

# 4. How all concepts connect

Here is the full rider event path:

```text
                       BOB MOVES TO CENTRAL
                                |
                                v
                         Node.js Producer
                                |
                                | sends topic=riders_update
                                v
                       +-------------------+
                       | riders_update     |
                       | Kafka Topic       |
                       +-------------------+
                                |
                    routing rule: central -> P1
                                |
                                v
                       +-------------------+
                       | Partition 1       |
                       | offset N          |
                       +-------------------+
                                |
                                | stored + replicated
                                v
                +--------------------------------+
                |        Kafka Cluster           |
                | kafka-1  kafka-2  kafka-3     |
                +--------------------------------+
                          |              |
                          |              |
              +-----------+              +-----------+
              v                                      v
        Consumer Group 1                       Consumer Group 2
        Dispatch                               Analytics
              |                                      |
      +-------+-------+                              |
      v       v       v                              v
     C-A     C-B     C-C                        analytics-a
      |       |       |                              |
     P0      P1      P2                         P0/P1/P2
              |
              | Bob's P1 record processed
              v
       group1 offset advances
```

That diagram is the bridge between all the terminology.

---

# 5. Project architecture

The lab combines a three-node Kafka KRaft cluster with a Node.js KafkaJS application.

```text
+---------------------------+
| Node.js Application       |
|                           |
| admin.js                  |---- creates topic
| producer.js               |---- sends rider events
| consumer.js               |---- reads rider events
| client.js                 |---- shared Kafka connection definition
| config.js                 |---- reads environment configuration
+-------------+-------------+
              |
              | Kafka protocol
              v
+------------------------------------------+
| Kafka KRaft Cluster                      |
|                                          |
| kafka-1   kafka-2   kafka-3             |
|                                          |
| topic: riders_update                     |
| P0 north | P1 central | P2 south         |
| RF=3, minISR=2                           |
+------------------------------------------+
```

---

# 6. Project folder structure

```text
kafka-one-stop-lab/
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
└── test/
    └── kafka.integration.test.js
```

---

# 7. How every file is connected

This is one of the most important project diagrams:

```text
                    .env
                     |
                     v
                config.js
                     |
                     | exports brokers, topic,
                     | partitions, RF, minISR...
                     v
                 client.js
                     |
                     | creates shared KafkaJS client
                     |
       +-------------+-------------+
       |             |             |
       v             v             v
    admin.js     producer.js    consumer.js
       |             |             |
       |             |             |
       v             v             v
 create topic    send records    read records

                    ^
                    |
               package.json
          provides easy npm commands

Docker Compose is underneath everything:

             Node.js files
                  |
                  v
          Kafka broker endpoints
                  |
                  v
            docker-compose.yml
          kafka-1 / kafka-2 / kafka-3
```

A more precise dependency view:

```text
.env.example -> copied to .env
       |
       v
config.js
       |
       +-----------------------------+
       |                             |
       v                             |
client.js                            |
       |                             |
       +----------+----------+-------+
                  |          |
                  v          v
               admin.js   producer.js
                  |
                  v
              consumer.js uses the same client.js too

integration test -> imports client.js -> therefore also uses config.js/.env
```

So there is only **one Kafka connection configuration** shared by admin, producer, consumer and tests.

---

# 8. Docker Compose: our Kafka cluster

File: `docker-compose.yml`

```yaml
services:
  kafka-1:
    image: apache/kafka:4.3.1
    hostname: kafka-1
    container_name: kafka-1
    ports:
      - "29092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
      KAFKA_LISTENERS: "PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-1:19092,PLAINTEXT_HOST://localhost:29092"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
    volumes:
      - kafka1-data:/var/lib/kafka/data

  kafka-2:
    image: apache/kafka:4.3.1
    hostname: kafka-2
    container_name: kafka-2
    ports:
      - "39092:9092"
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
      KAFKA_LISTENERS: "PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-2:19092,PLAINTEXT_HOST://localhost:39092"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
    volumes:
      - kafka2-data:/var/lib/kafka/data

  kafka-3:
    image: apache/kafka:4.3.1
    hostname: kafka-3
    container_name: kafka-3
    ports:
      - "49092:9092"
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
      KAFKA_LISTENERS: "PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-3:19092,PLAINTEXT_HOST://localhost:49092"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
    volumes:
      - kafka3-data:/var/lib/kafka/data

volumes:
  kafka1-data:
  kafka2-data:
  kafka3-data:
```

## What this file creates

It creates:

```text
kafka-1
kafka-2
kafka-3
```

Each node is configured as:

```text
broker + KRaft controller
```

## Important configuration explained

### `KAFKA_NODE_ID`

Unique identity of each Kafka node:

```text
kafka-1 -> 1
kafka-2 -> 2
kafka-3 -> 3
```

### `KAFKA_PROCESS_ROLES`

```text
broker,controller
```

For the lab, each node stores data and participates in KRaft metadata quorum.

### `KAFKA_CONTROLLER_QUORUM_VOTERS`

```text
1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

This tells every node who the KRaft controllers are.

### `KAFKA_LISTENERS`

There are multiple listener purposes:

```text
PLAINTEXT       -> broker-to-broker/internal Docker network
CONTROLLER      -> KRaft controller communication
PLAINTEXT_HOST  -> access from your Windows/host machine
```

### `KAFKA_ADVERTISED_LISTENERS`

For `kafka-1`:

```text
PLAINTEXT://kafka-1:19092
PLAINTEXT_HOST://localhost:29092
```

Inside Docker, services can know it as `kafka-1:19092`.

From your host Node.js process, it is reachable as `localhost:29092`.

Similarly:

```text
kafka-1 -> localhost:29092
kafka-2 -> localhost:39092
kafka-3 -> localhost:49092
```

### Replication defaults

```text
KAFKA_DEFAULT_REPLICATION_FACTOR=3
KAFKA_MIN_INSYNC_REPLICAS=2
```

This lab deliberately supports a durability experiment.

### `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`

Kafka will not silently create a topic just because a producer made a typo.

We create topics deliberately through `admin.js`.

### Volumes

```text
kafka1-data
kafka2-data
kafka3-data
```

These keep Kafka data persistent across a normal container restart.

`docker compose down -v` removes them and resets the lab completely.

---

# 9. `.env` and application configuration

The supplied `.env.example` is:

```dotenv
KAFKA_BROKERS=localhost:29092,localhost:39092,localhost:49092
KAFKA_CLIENT_ID=kafka-one-stop-lab
KAFKA_TOPIC=riders_update
KAFKA_PARTITIONS=3
KAFKA_REPLICATION_FACTOR=3
KAFKA_MIN_ISR=2
```

Copy it before running the Node.js application:

```bash
cp .env.example .env
```

It means:

```text
KAFKA_BROKERS
  Node.js can initially contact any of these three broker endpoints.

KAFKA_CLIENT_ID
  Human-readable application/client identifier used by KafkaJS.

KAFKA_TOPIC
  Main topic used by this project.

KAFKA_PARTITIONS
  Number of partitions admin.js creates.

KAFKA_REPLICATION_FACTOR
  Number of broker replicas for every partition.

KAFKA_MIN_ISR
  Minimum number of in-sync replicas required by durable writes.
```

---

# 10. `config.js`

File: `src/config.js`

```javascript
require("dotenv").config();

function positiveInt(name, fallback) {
  const n = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(n) || n < 1) throw new Error(`${name} must be a positive integer`);
  return n;
}

module.exports = {
  brokers: (process.env.KAFKA_BROKERS || "localhost:29092,localhost:39092,localhost:49092")
    .split(",").map(v => v.trim()).filter(Boolean),
  clientId: process.env.KAFKA_CLIENT_ID || "kafka-one-stop-lab",
  topic: process.env.KAFKA_TOPIC || "riders_update",
  partitions: positiveInt("KAFKA_PARTITIONS", 3),
  replicationFactor: positiveInt("KAFKA_REPLICATION_FACTOR", 3),
  minIsr: positiveInt("KAFKA_MIN_ISR", 2)
};
```

## What it does

This file is the bridge from environment variables into JavaScript.

```text
.env
 |
 v
config.js
 |
 +-> brokers
 +-> clientId
 +-> topic
 +-> partitions
 +-> replicationFactor
 `-> minIsr
```

Every other application file does **not** need to independently parse environment variables.

The helper:

```javascript
positiveInt(...)
```

prevents invalid values such as `0`, negative numbers or non-integers for partition/replication settings.

---

# 11. `client.js`

File: `src/client.js`

```javascript
const { Kafka, logLevel } = require("kafkajs");
const config = require("./config");

const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers,
  connectionTimeout: 5000,
  requestTimeout: 30000,
  retry: { initialRetryTime: 300, retries: 8 },
  logLevel: logLevel.INFO
});

module.exports = { kafka, config };
```

## Why this file exists

Without `client.js`, `admin.js`, `producer.js`, `consumer.js` and the test could each duplicate Kafka connection configuration.

Instead:

```text
config.js
   |
   v
client.js
   |
   +-> admin.js
   +-> producer.js
   +-> consumer.js
   `-> integration test
```

This line imports KafkaJS:

```javascript
const { Kafka, logLevel } = require("kafkajs");
```

This line imports our environment-driven configuration:

```javascript
const config = require("./config");
```

Then:

```javascript
const kafka = new Kafka({ ... });
```

creates the reusable KafkaJS client definition.

The `brokers` list is only for bootstrapping. After connecting, Kafka gives the client metadata about the rest of the cluster.

---

# 12. `admin.js`

File: `src/admin.js`

```javascript
const { kafka, config } = require("./client");

async function createTopic(topicName = config.topic) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [{
        topic: topicName,
        numPartitions: config.partitions,
        replicationFactor: config.replicationFactor,
        configEntries: [
          { name: "min.insync.replicas", value: String(config.minIsr) },
          { name: "cleanup.policy", value: "delete" },
          { name: "retention.ms", value: "604800000" }
        ]
      }]
    });
    console.log(created
      ? `Created "${topicName}" partitions=${config.partitions} RF=${config.replicationFactor} minISR=${config.minIsr}`
      : `Topic "${topicName}" already exists`);
  } finally {
    await admin.disconnect();
  }
}

if (require.main === module) {
  createTopic().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { createTopic };
```

## What admin.js does

It creates the topic before producers and consumers use it.

The dependency is:

```text
.env
  -> config.js
      -> client.js
          -> admin.js
```

This line:

```javascript
const { kafka, config } = require("./client");
```

gives `admin.js` both the Kafka client and configuration values.

Then:

```javascript
const admin = kafka.admin();
```

creates an administrative KafkaJS client.

The topic configuration comes from `.env` through `config.js`:

```text
topic              = riders_update
partitions         = 3
replicationFactor  = 3
minISR             = 2
```

It also sets:

```text
cleanup.policy = delete
retention.ms   = 604800000
```

`604800000` ms is seven days.

Run it with:

```bash
npm run topic:create
```

or directly:

```bash
node src/admin.js
```

---

# 13. `producer.js`

File: `src/producer.js`

```javascript
const readline = require("readline");
const crypto = require("crypto");
const { kafka, config } = require("./client");

const locationToPartition = { north: 0, central: 1, south: 2 };

function partitionFor(location) {
  const key = String(location || "").toLowerCase();
  if (!(key in locationToPartition)) {
    throw new Error("Location must be north, central, or south");
  }
  return locationToPartition[key];
}

function buildMessage(riderName, location, sequence = null) {
  const normalized = String(location).toLowerCase();
  return {
    partition: partitionFor(normalized),
    key: String(riderName),
    headers: {
      "event-type": "rider-location-updated",
      "content-type": "application/json"
    },
    value: JSON.stringify({
      eventId: crypto.randomUUID(),
      riderName,
      location: normalized,
      sequence,
      occurredAt: new Date().toISOString()
    })
  };
}

async function send(messages, topic = config.topic) {
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
    maxInFlightRequests: 5
  });
  await producer.connect();
  try {
    return await producer.send({ topic, acks: -1, messages });
  } finally {
    await producer.disconnect();
  }
}

async function demo(count) {
  const locations = ["north", "central", "south"];
  const messages = Array.from({ length: count }, (_, i) =>
    buildMessage(`rider-${i + 1}`, locations[i % 3], i + 1));
  await send(messages);
  console.log(`Produced ${count} messages to ${config.topic}`);
}

async function interactive() {
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
    maxInFlightRequests: 5
  });
  await producer.connect();

  console.log(`Topic: ${config.topic}`);
  console.log("Enter: <riderName> <north|central|south>");

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "> "
  });
  rl.prompt();

  rl.on("line", async line => {
    try {
      const [name, location] = line.trim().split(/\s+/);
      if (!name || !location) throw new Error("Expected: <riderName> <location>");
      const message = buildMessage(name, location);
      await producer.send({ topic: config.topic, acks: -1, messages: [message] });
      console.log(`sent key=${name} location=${location} partition=${message.partition}`);
    } catch (err) {
      console.error(err.message);
    } finally {
      rl.prompt();
    }
  });

  const shutdown = async () => {
    rl.close();
    await producer.disconnect();
    process.exit(0);
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

async function main() {
  const args = process.argv.slice(2);
  const i = args.indexOf("--demo");
  if (i >= 0) {
    const count = Number(args[i + 1] || 12);
    if (!Number.isInteger(count) || count < 1) throw new Error("Demo count must be positive");
    await demo(count);
  } else {
    await interactive();
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { buildMessage, partitionFor, send };
```

## 13.1 Where producer.js gets Kafka from

```text
producer.js
    |
    v
client.js
    |
    v
config.js
    |
    v
.env
```

This line creates the connection reference:

```javascript
const { kafka, config } = require("./client");
```

## 13.2 Routing map

This is our teaching routing rule:

```javascript
const locationToPartition = { north: 0, central: 1, south: 2 };
```

Therefore:

```text
north   -> P0
central -> P1
south   -> P2
```

## 13.3 `partitionFor()`

This function validates the location and returns the correct partition number.

Bob:

```text
partitionFor("central") -> 1
```

## 13.4 `buildMessage()`

This function creates the complete Kafka record:

```text
partition
key
headers
value
```

Example conceptually:

```json
{
  "partition": 1,
  "key": "bob",
  "headers": {
    "event-type": "rider-location-updated",
    "content-type": "application/json"
  },
  "value": {
    "riderName": "bob",
    "location": "central"
  }
}
```

The actual `value` sent to Kafka is a JSON string.

## 13.5 Interactive mode

Run:

```bash
npm run producer
```

Then type:

```text
bob central
alice north
carol south
```

The producer stays open and waits for more input.

## 13.6 Demo mode

Run:

```bash
npm run producer:demo
```

This sends 12 generated events cycling through north/central/south.

## 13.7 Durability settings

The producer uses:

```javascript
idempotent: true
```

and sends with:

```javascript
acks: -1
```

In KafkaJS, `-1` means all required in-sync replicas must acknowledge according to Kafka's durability rules.

Together with:

```text
RF=3
minISR=2
```

this gives us the broker-failure experiment later.

---

# 14. `consumer.js`

File: `src/consumer.js`

```javascript
const { kafka, config } = require("./client");

async function startConsumer(groupId, consumerName) {
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    allowAutoTopicCreation: false
  });

  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: true });

  console.log(`consumer=${consumerName} group=${groupId} topic=${config.topic}`);

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const headers = Object.fromEntries(
        Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString()])
      );
      console.log(JSON.stringify({
        consumer: consumerName,
        groupId,
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        headers,
        value: JSON.parse(message.value.toString())
      }, null, 2));
    }
  });

  const shutdown = async () => {
    await consumer.disconnect();
    process.exit(0);
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

if (require.main === module) {
  const groupId = process.argv[2] || "group1";
  const consumerName = process.argv[3] || `consumer-${process.pid}`;
  startConsumer(groupId, consumerName).catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { startConsumer };
```

## 14.1 Command-line arguments

This is especially important:

```javascript
const groupId = process.argv[2] || "group1";
const consumerName = process.argv[3] || `consumer-${process.pid}`;
```

So this command:

```bash
node src/consumer.js group3 analytics-b
```

means:

```text
process.argv[2] = group3
process.argv[3] = analytics-b
```

Therefore:

```text
groupId      = group3
consumerName = analytics-b
```

## 14.2 Creating the consumer

```javascript
const consumer = kafka.consumer({
  groupId,
  ...
});
```

This is where the consumer joins a Kafka **consumer group**.

Consumers with the same `groupId` cooperate.

Consumers with different `groupId`s consume independently.

## 14.3 Subscription

```javascript
await consumer.subscribe({
  topic: config.topic,
  fromBeginning: true
});
```

The consumer subscribes to `riders_update`.

`fromBeginning: true` is relevant when there is no applicable committed offset for that group. If the group already has valid committed offsets, Kafka normally resumes according to those offsets.

## 14.4 `consumer.run()`

The consumer continuously polls Kafka and invokes `eachMessage` for received records.

For every record our program prints:

```text
consumer name
group ID
topic
partition
offset
key
headers
value
```

That is why this lab is useful: you can directly see the Kafka metadata instead of hiding it behind business logic.

## 14.5 Graceful shutdown

The code handles:

```text
SIGINT  -> Ctrl+C
SIGTERM -> termination signal
```

and disconnects the consumer.

When a consumer disconnects from a group, Kafka may rebalance the remaining members.

---

# 15. `package.json`

File: `package.json`

```json
{
  "name": "kafka-one-stop-lab",
  "version": "1.0.0",
  "private": true,
  "description": "Apache Kafka theory + Node.js implementation lab with KRaft, consumer groups, testing and production guidance.",
  "scripts": {
    "topic:create": "node src/admin.js",
    "producer": "node src/producer.js",
    "producer:demo": "node src/producer.js --demo 12",
    "consumer:group1:a": "node src/consumer.js group1 consumer-a",
    "consumer:group1:b": "node src/consumer.js group1 consumer-b",
    "consumer:group2": "node src/consumer.js group2 analytics-a",
    "test:integration": "node --test test/kafka.integration.test.js",
    "check": "node --check src/config.js && node --check src/client.js && node --check src/admin.js && node --check src/producer.js && node --check src/consumer.js && node --check test/kafka.integration.test.js"
  },
  "engines": { "node": ">=20" },
  "dependencies": {
    "dotenv": "^16.4.7",
    "kafkajs": "^2.2.4"
  }
}
```

## What package.json gives us

Dependencies:

```text
kafkajs -> Node.js Kafka client
 dotenv  -> reads .env
```

Useful scripts:

```text
npm run topic:create
  -> node src/admin.js

npm run producer
  -> node src/producer.js

npm run producer:demo
  -> node src/producer.js --demo 12

npm run consumer:group1:a
  -> node src/consumer.js group1 consumer-a

npm run consumer:group1:b
  -> node src/consumer.js group1 consumer-b

npm run consumer:group2
  -> node src/consumer.js group2 analytics-a

npm run test:integration
  -> Node's built-in test runner executes the Kafka integration test
```

So `package.json` is mostly a convenient command layer over the real JavaScript files.

---

# 16. Start the complete lab

## Step 1 — start Kafka

From the project root:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

You want all three brokers running:

```text
kafka-1
kafka-2
kafka-3
```

## Step 2 — create `.env`

```bash
cp .env.example .env
```

## Step 3 — install Node packages

```bash
npm install
```

## Step 4 — syntax check

```bash
npm run check
```

## Step 5 — create topic

```bash
npm run topic:create
```

Expected conceptually:

```text
Created "riders_update" partitions=3 RF=3 minISR=2
```

---

# 17. Create and inspect the topic

List topics from inside a Kafka container:

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server kafka-1:19092   --list
```

Describe the rider topic:

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server kafka-1:19092   --describe   --topic riders_update
```

You should see three partitions and replication information.

---

# 18. Produce rider events

Start the interactive producer:

```bash
npm run producer
```

Input:

```text
alice north
bob central
carol south
```

Expected routing:

```text
alice -> P0
bob   -> P1
carol -> P2
```

Or generate 12 test records:

```bash
npm run producer:demo
```

The demo cycles:

```text
north, central, south, north, central, south ...
```

---

# 19. Consume rider events

Open a new terminal:

```bash
node src/consumer.js group1 consumer-a
```

With only one consumer in `group1`, it can own all three partitions:

```text
group1
  consumer-a
     ├── P0
     ├── P1
     └── P2
```

When records arrive, output contains data such as:

```text
consumer  = consumer-a
groupId   = group1
topic     = riders_update
partition = 1
offset    = 4
key       = bob
value     = { riderName: bob, location: central }
```

---

# 20. Consumer groups and partition sharing

## Experiment A — one consumer

Run:

```bash
node src/consumer.js group1 consumer-a
```

Result conceptually:

```text
P0 --+
P1 --+--> consumer-a
P2 --+
```

## Experiment B — two consumers in the same group

Terminal 1:

```bash
node src/consumer.js group1 consumer-a
```

Terminal 2:

```bash
node src/consumer.js group1 consumer-b
```

Now Kafka must distribute 3 partitions across 2 consumers.

Possible result:

```text
consumer-a -> P0, P2
consumer-b -> P1
```

The exact assignment may vary.

## Experiment C — three consumers in the same group

Add:

```bash
node src/consumer.js group1 consumer-c
```

Possible result:

```text
consumer-a -> P0
consumer-b -> P1
consumer-c -> P2
```

## Experiment D — four consumers in the same group

Add:

```bash
node src/consumer.js group1 consumer-d
```

Now:

```text
partitions = 3
consumers  = 4
```

One consumer must be idle.

## Experiment E — another consumer group

Run:

```bash
node src/consumer.js group2 analytics-a
```

`group2` is independent of `group1`.

If `group2` has one consumer, it can own all three partitions:

```text
P0 --+
P1 --+--> analytics-a
P2 --+
```

At the same time, `group1` may be sharing the same topic among several consumers.

This is the core distinction:

```text
within one group -> load sharing
between groups   -> independent processing
```

---

# 21. Rebalancing experiment

Start only one member:

```bash
node src/consumer.js group3 analytics-a
```

It may log:

```text
memberAssignment: riders_update [0,1,2]
```

because it is the only member.

Now start another:

```bash
node src/consumer.js group3 analytics-b
```

Kafka sees membership change and rebalances.

Conceptually:

```text
Before:
analytics-a -> P0,P1,P2

After analytics-b joins:
analytics-a -> some partitions
analytics-b -> remaining partitions
```

Stop `analytics-b` using Ctrl+C.

Kafka rebalances again and returns its partitions to active members.

So:

```text
consumer joins/leaves
       |
       v
membership changes
       |
       v
Kafka reassigns partitions
       |
       v
REBAlANCE
```

---

# 22. Offsets and lag experiment

List groups:

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --list
```

Example:

```text
group1
group2
group3
```

This means Kafka knows about those group IDs. It does **not** necessarily mean every group currently has a live consumer.

Describe `group1`:

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --describe   --group group1
```

Typical columns:

```text
GROUP
TOPIC
PARTITION
CURRENT-OFFSET
LOG-END-OFFSET
LAG
CONSUMER-ID
HOST
CLIENT-ID
```

Meaning:

```text
GROUP
  which consumer group

TOPIC
  which topic

PARTITION
  which partition

CURRENT-OFFSET
  committed progress of this group for the partition

LOG-END-OFFSET
  current end position of that partition

LAG
  how far the group is behind

CONSUMER-ID
  Kafka's unique ID for the currently assigned consumer member

HOST
  network address Kafka sees for that member

CLIENT-ID
  client ID configured by our KafkaJS application
```

If you see:

```text
CURRENT-OFFSET = 14
LOG-END-OFFSET = 14
LAG = 0
```

then the group is completely caught up.

---

# 23. Why an inactive group still appears

Suppose:

```bash
--list
```

shows:

```text
group3
```

but:

```bash
--describe --group group3
```

shows:

```text
Consumer group 'group3' has no active members.
```

and:

```text
CONSUMER-ID = -
HOST        = -
CLIENT-ID   = -
```

This means:

```text
group3 still exists as Kafka group state
+
Kafka still remembers its committed offsets
+
there is no running consumer connected to group3 right now
```

Think:

```text
group3
├── saved P0 offset
├── saved P1 offset
├── saved P2 offset
└── active consumer = none
```

A consumer ID represents a **live member**, not the permanent group itself.

---

# 24. CLI commands for consumer groups

## List groups

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --list
```

## Describe group1

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --describe   --group group1
```

## Describe group2

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --describe   --group group2
```

## Describe group3

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka-1   /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --describe   --group group3
```

---

# 25. Git Bash path-conversion note

On Windows Git Bash, this can fail:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh ...
```

because Git Bash may convert:

```text
/opt/kafka/...
```

into a Windows path such as:

```text
C:/Program Files/Git/opt/kafka/...
```

Use:

```bash
MSYS_NO_PATHCONV=1 docker exec ...
```

when passing Linux absolute paths directly to `docker exec`.

Or enter the container first:

```bash
docker exec -it kafka-1 sh
```

Then, inside the Linux container:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka-1:19092   --list
```

---

# 26. Broker replication and failure experiment

Our important durability settings are:

```text
replication factor = 3
min ISR            = 2
producer acks      = all
```

Conceptually a partition can look like:

```text
Partition P1

Broker 1 -> leader
Broker 2 -> follower/in-sync replica
Broker 3 -> follower/in-sync replica
```

## Stop one broker

```bash
docker stop kafka-3
```

Then produce:

```bash
npm run producer:demo
```

If two in-sync replicas remain, the cluster can still satisfy `minISR=2`.

## Stop a second broker

```bash
docker stop kafka-2
```

Try:

```bash
npm run producer:demo
```

Now only one broker is available, so a write requiring at least two in-sync replicas should fail rather than be acknowledged with insufficient durability.

Restore:

```bash
docker start kafka-2 kafka-3
```

This experiment connects three concepts:

```text
Replication Factor
        +
min.insync.replicas
        +
producer acks=all
        |
        v
write durability behavior during broker failure
```

---

# 27. Automated integration test

File: `test/kafka.integration.test.js`

```javascript
const test = require("node:test");
const assert = require("node:assert/strict");
const { kafka, config } = require("../src/client");

async function consumeN(topic, groupId, expected) {
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  const received = [];
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  let resolveDone, rejectDone;
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });

  const timer = setTimeout(() => {
    rejectDone(new Error(`${groupId} received ${received.length}/${expected}`));
  }, 30000);

  consumer.run({
    eachMessage: async ({ partition, message }) => {
      received.push({
        partition,
        value: JSON.parse(message.value.toString())
      });
      if (received.length >= expected) {
        clearTimeout(timer);
        resolveDone(received);
      }
    }
  }).catch(rejectDone);

  return { consumer, done };
}

test("different consumer groups independently receive the same stream", { timeout: 45000 }, async () => {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const topic = `kafka-one-stop-test-${suffix}`;
  const count = 9;
  const admin = kafka.admin();
  const producer = kafka.producer({ idempotent: true, allowAutoTopicCreation: false });

  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: [{
      topic,
      numPartitions: 3,
      replicationFactor: Math.min(3, config.replicationFactor),
      configEntries: [{ name: "min.insync.replicas", value: String(Math.min(2, config.minIsr)) }]
    }]
  });

  const a = await consumeN(topic, `group-a-${suffix}`, count);
  const b = await consumeN(topic, `group-b-${suffix}`, count);

  try {
    await producer.connect();
    await producer.send({
      topic,
      acks: -1,
      messages: Array.from({ length: count }, (_, i) => ({
        partition: i % 3,
        key: `rider-${i + 1}`,
        value: JSON.stringify({ id: i + 1 })
      }))
    });

    const [ra, rb] = await Promise.all([a.done, b.done]);
    assert.equal(new Set(ra.map(x => x.value.id)).size, count);
    assert.equal(new Set(rb.map(x => x.value.id)).size, count);
  } finally {
    await a.consumer.disconnect().catch(() => {});
    await b.consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await admin.deleteTopics({ topics: [topic] }).catch(() => {});
    await admin.disconnect().catch(() => {});
  }
});
```

Run it with all brokers healthy:

```bash
npm run test:integration
```

The test proves a core Kafka behavior:

```text
Create temporary topic
        |
        v
3 partitions
        |
        +------------------+
        |                  |
        v                  v
consumer group A     consumer group B
        |                  |
        +--------+---------+
                 |
                 v
          producer sends 9
                 |
                 v
both groups independently receive all 9
```

---

# 28. How the test code works

## `consumeN()`

This helper starts a consumer with the supplied group ID:

```javascript
kafka.consumer({ groupId, ... })
```

It subscribes to the temporary topic and stores received records in an array.

When it receives the expected number, it resolves a Promise.

## Main test

The test creates a unique topic name using a timestamp/random suffix. That prevents collisions with previous runs.

It creates:

```text
3 partitions
replication factor up to 3
minISR up to 2
```

Then it starts:

```text
group A
group B
```

These are different group IDs, so they are independent.

The producer sends 9 records explicitly distributed:

```text
message 1 -> P0
message 2 -> P1
message 3 -> P2
message 4 -> P0
...
```

Finally it asserts that both groups saw all 9 unique IDs.

The test then disconnects consumers, producer and admin client and deletes the temporary topic.

---

# 29. Complete end-to-end request flow

Now follow Bob's event through the **actual project files**.

```text
1. User types:
   bob central

2. producer.js receives the line
       |
       v
3. buildMessage("bob", "central")
       |
       +-> partitionFor("central") = 1
       +-> key = "bob"
       +-> headers added
       +-> JSON value created
       |
       v
4. producer.send({
      topic: config.topic,
      acks: -1,
      messages: [message]
   })
       |
       v
5. config.topic came from:
   .env -> config.js -> client.js -> producer.js
       |
       v
6. Kafka receives record for riders_update / P1
       |
       v
7. P1 leader broker appends record at next offset
       |
       v
8. P1 is replicated to other brokers according to RF=3
       |
       v
9. consumer.js group1 members subscribe to riders_update
       |
       v
10. Kafka group coordinator assigns P1 to one group1 consumer
       |
       v
11. that consumer's eachMessage() runs
       |
       v
12. application prints:
    topic, partition, offset, key, headers, value
       |
       v
13. group1's committed progress advances
```

Now add `group2`:

```text
The same stored Bob event
       |
       +-> group1 consumes it according to group1 offsets
       |
       `-> group2 independently consumes it according to group2 offsets
```

That is Kafka's event-stream model in one example.

---

# 30. Common confusions cleared up

## "Is a broker the same as a topic?"

No.

```text
Broker    = server
Topic     = logical event stream
Partition = subdivision of a topic stored on brokers
```

## "Does a producer send to a broker or a topic?"

At application level, the producer publishes to a **topic**. Kafka metadata tells the client which broker currently leads the selected partition, and the client communicates with that broker.

## "Does every consumer receive every message?"

Not if consumers are in the **same group**.

Same group:

```text
share partitions/work
```

Different groups:

```text
independently consume the topic
```

## "Is an offset the message ID?"

Not globally.

It is a position within a particular partition.

```text
P0 offset 5
```

and:

```text
P1 offset 5
```

are two different positions.

## "Is CURRENT-OFFSET simply the last message number?"

Think of it as the group's committed progress/next position, not simply a count of business events.

## "Why does a group appear with no consumer ID?"

Because Kafka still knows the group's committed offsets even though no member of the group is currently connected.

## "Why is the same consumer ID shown for P0, P1 and P2?"

Because that group currently has one active consumer, so the same consumer owns all three partitions.

## "Why does Kafka say rebalancing?"

Group membership is changing, so Kafka is recalculating partition ownership.

## "Does LAG=0 mean the consumer is healthy forever?"

It means it is caught up at that moment. You still need to monitor application errors, processing latency and future lag growth.

## "Why do we need three brokers if we already have three partitions?"

Partition count and broker count solve different problems:

```text
partitions -> parallelism/scaling of a topic
brokers    -> servers/storage/cluster capacity
replicas   -> fault tolerance across brokers
```

A partition can have replicas across several brokers.

---

# 31. From this lab to production Kafka

The Docker Compose cluster is for learning. Do not deploy it unchanged to production.

A gradual production path is:

```text
Local lab
  |
  v
Define throughput, event size, retention, SLO, RPO/RTO
  |
  v
Choose managed Kafka or self-managed Kafka
  |
  v
Production KRaft topology
  |
  +-> 3 dedicated controllers
  `-> 3+ dedicated brokers
  |
  v
Persistent high-performance storage
  |
  v
Private networking + correct advertised.listeners
  |
  v
TLS encryption
  |
  v
Authentication
  |
  v
Least-privilege ACL authorization
  |
  v
Topic governance
  |
  +-> partitions
  +-> RF
  +-> minISR
  +-> retention
  +-> cleanup policy
  +-> event key
  `-> schema
  |
  v
Durable producer settings
  |
  +-> acks=all
  +-> idempotence
  `-> retries/timeouts
  |
  v
Reliable consumer design
  |
  +-> stable group IDs
  +-> idempotent processing
  +-> retry/DLQ strategy where appropriate
  `-> lag monitoring
  |
  v
Observability
  |
  +-> broker health
  +-> controller health
  +-> ISR / under-replicated partitions
  +-> request latency/errors
  +-> disk/network
  `-> consumer lag
  |
  v
Load + failure testing
  |
  v
DR plan
  |
  v
Staging
  |
  v
Production readiness gate
  |
  v
Controlled production cutover
```

A common durability starting pattern for important topics is:

```text
replication.factor = 3
min.insync.replicas = 2
producer acks       = all
```

Production should also have:

```text
TLS
service authentication
ACLs
secret management
monitoring/alerting
schema compatibility rules
capacity planning
backups/DR strategy
runbooks
rolling upgrade procedure
```

A conservative final rollout sequence:

```text
1. Deploy KRaft controllers
2. Verify quorum
3. Deploy brokers
4. Verify storage and network
5. Verify TLS
6. Verify authentication
7. Apply/test ACLs
8. Create topics with reviewed policies
9. Verify leaders/replicas/ISR
10. Deploy consumers
11. Deploy producers
12. Start controlled traffic
13. Observe errors, latency, ISR, lag and rebalances
14. Ramp traffic gradually
15. Confirm SLOs
16. Record deployment evidence and runbooks
```

---

# 32. Final cheat sheet

| Term | Meaning in this rider project |
|---|---|
| Producer | Node.js process that sends rider updates |
| Cluster | All Kafka nodes together |
| Broker | One Kafka server, e.g. `kafka-1` |
| Topic | `riders_update` |
| Partition | P0 north, P1 central, P2 south in this teaching project |
| Record | One rider-location event |
| Header | Metadata such as `event-type` |
| Key | Rider identity used on the Kafka record |
| Value | JSON rider event payload |
| Routing | Choosing P0/P1/P2 |
| Offset | Position inside a partition |
| Consumer | One running process reading events |
| Consumer Group | Consumers cooperating under one `groupId` |
| Rebalance | Redistributing partitions when group membership changes |
| Lag | How far a group is behind the partition end |
| Replication Factor | Number of copies of a partition across brokers |
| ISR | Replicas currently sufficiently in sync |
| KRaft | Kafka's metadata/controller mechanism used instead of ZooKeeper |

The single sentence that connects everything is:

> **The producer publishes a rider event to a topic; the event is routed to a partition whose leader is on a Kafka broker; Kafka stores and replicates it; each consumer group independently reads the topic; Kafka assigns each partition to one consumer inside each group; offsets remember each group's progress; and a rebalance changes those assignments when consumers join or leave.**

---

# Appendix A — Exact project files

The code shown above is the complete code used by this lab. For quick reference, the relationship is:

```text
docker-compose.yml
  -> creates Kafka cluster

.env
  -> supplies application settings

config.js
  -> parses .env

client.js
  -> builds common KafkaJS client

admin.js
  -> creates topic using client.js + config

producer.js
  -> publishes events using client.js + config

consumer.js
  -> consumes events using client.js + config

kafka.integration.test.js
  -> creates temporary topic + producer + independent groups to verify behavior
```

# Appendix B — Full clean reset

Stop and remove containers:

```bash
docker compose down
```

Stop and also remove Kafka data volumes:

```bash
docker compose down -v
```

Then rebuild the learning environment from zero:

```bash
docker compose up -d
npm run topic:create
```

# Appendix C — Recommended experiment order

For the clearest learning sequence, perform the lab in this order:

```text
1. Start brokers
2. Create topic
3. Run one consumer in group1
4. Send three manual rider events
5. Inspect partition and offset values
6. Add second consumer to group1
7. Observe rebalance
8. Add third consumer to group1
9. Observe one partition per consumer
10. Start group2
11. Prove different groups consume independently
12. List/describe consumer groups
13. Stop a consumer and observe blank CONSUMER-ID after it leaves
14. Restart the group and observe assignment again
15. Stop one broker
16. Produce successfully with two ISR remaining
17. Stop another broker
18. Observe durable write failure
19. Restore brokers
20. Run automated integration test
```

After these experiments, Kafka terms should no longer be separate vocabulary. They are parts of one event flow.
