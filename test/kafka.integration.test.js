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
