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
