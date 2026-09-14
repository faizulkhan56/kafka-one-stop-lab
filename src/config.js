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
