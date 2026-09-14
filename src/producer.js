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
