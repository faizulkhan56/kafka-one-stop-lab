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
