const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const { Kafka } = require("kafkajs");

dotenv.config();

const KAFKA_BOOTSTRAP_SERVER_URL =
  process.env.KAFKA_BOOTSTRAP_SERVER_URL ||
  "my-cluster-kafka-bootstrap.kafka:9092";
const KAFKA_USERNAME = process.env.KAFKA_USERNAME || "my-connect-user";
const KAFKA_PASSWORD =
  process.env.KAFKA_PASSWORD || "eWKhGtJJ16Fo9svPInU8Osw99zEZ44wt";
const SERVICE_PORT = process.env.SERVICE_PORT || 8081;

const kafka = new Kafka({
  brokers: [KAFKA_BOOTSTRAP_SERVER_URL],
  sasl: {
    mechanism: "scram-sha-512",
    username: KAFKA_USERNAME,
    password: KAFKA_PASSWORD,
  },
});

// Create producer
const producer = kafka.producer();

// Connect producer
const connectProducer = async () => {
  try {
    await producer.connect();
    console.log("Producer connected successfully");
  } catch (error) {
    console.error("Error connecting producer:", error);
  }
};

connectProducer();

const gracefulShutdown = async () => {
  await producer.disconnect();
};

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());
app.use(cors());

let isPublishing = false;
let publishInterval = null;
let pacemakerData;

let loopCount = 0; // Initialize a counter

app.post("/start", async (req, res) => {
  let { start } = req.body;
  if (start) {
    startPublishing();
    return res.status(200).json({ message: "Publishing started" });
  } else {
    stopPublishing();
    return res.status(200).json({ message: "Publishing stopped" });
  }
});

app.get("/getData",async (req, res)=>{
  let data = await generateRandomData();
  return res.status(200).send({
    code:200,
    status:"OK",
    data:data
  })
});

const generateRandomData = () => {
  loopCount++; // Increment the counter on every call

  const heartRate = Math.floor(Math.random() * (120 - 60 + 1)) + 60;

  return {
    deviceId: `PACEMAKER-${Math.floor(Math.random() * 1000) + 1}`,
    timestamp: new Date().toISOString(),
    heartRate,
    batteryStatus: Math.max(0, Math.min(100, (Math.random() * 100).toFixed(1))),
    pacingMode: ["DDD", "AAI", "VVI"][Math.floor(Math.random() * 3)],
    pacingRate: Math.floor(Math.random() * (90 - 60 + 1)) + 60,
    anomalies: {
      arrhythmiaDetected: Math.random() < 0.05,
      highHeartRate: loopCount % 20 === 0, // Trigger on every 20th loop
      lowHeartRate: heartRate < 70,
    },
    geoLocation: {
      latitude: (Math.random() * 180 - 90).toFixed(6),
      longitude: (Math.random() * 360 - 180).toFixed(6),
    },
  };
};

const startPublishing = async () => {
  if (isPublishing) return;
  isPublishing = true;
  console.log("Kafka producer started");

  publishInterval = setInterval(async () => {
    pacemakerData = generateRandomData();

    const payload = {
      topic: process.env.PUBLISH_TOPIC,
      messages: [{ value: JSON.stringify(pacemakerData) }],
    };

    try {
      await producer.send(payload);
      console.log("Published:", pacemakerData);
    } catch (err) {
      console.error("Error publishing to Kafka:", err);
    }
  }, 2000);
};

const stopPublishing = () => {
  if (!isPublishing) return;
  isPublishing = false;
  clearInterval(publishInterval);
  console.log("Kafka producer stopped");
};

app.listen(SERVICE_PORT, function () {
  console.log(`Server listening on port ${SERVICE_PORT}`);
});
