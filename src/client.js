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
