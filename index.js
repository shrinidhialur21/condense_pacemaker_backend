const { Kafka } = require("kafkajs");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
 
const KAFKA_BOOTSTRAP_SERVER_URL = "my-cluster-kafka-bootstrap.kafka:9092";
const KAFKA_USERNAME = "my-connect-user";
const KAFKA_PASSWORD = "eWKhGtJJ16Fo9svPInU8Osw99zEZ44wt";


// Kafka configuration
const kafka = new Kafka({
  brokers: [KAFKA_BOOTSTRAP_SERVER_URL],
  ssl: true,
  sasl: {
    mechanism: "scram-sha-512",
    username: KAFKA_USERNAME,
    password: KAFKA_PASSWORD
  }
});

// Create producer
const producer = kafka.producer();

// Connect producer
const connectProducer = async () => {
  try {
    await producer.connect();
    console.log('Producer connected successfully');
  } catch (error) {
    console.error('Error connecting producer:', error);
  }
};


connectProducer();

const gracefulShutdown = async () => {
  await producer.disconnect();
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);
 
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: true, // Replace with your frontend's URL
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type"], // Allow custom headers
  },
});
 
const corsOptions = {
  origin: "http://localhost:5173", // Replace with your frontend's URL
  methods: ["GET", "POST"],
  allowedHeaders: ["Content-Type"],
  credentials: true, // if you need to send cookies across domains
};
 
app.use(cors(corsOptions));
 
let isPublishing = false;
let publishInterval = null;
let pacemakerData;
 
let loopCount = 0; // Initialize a counter
 
// Generate random pacemaker data
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
 
// Start publishing data
const startPublishing = async () => {
  if (isPublishing) return;
  isPublishing = true;
  console.log("Kafka producer started");
 
  publishInterval = setInterval(async () => {
    pacemakerData = generateRandomData();
 
    const payload = {
      topic: "pacemaker-data",
      messages: [{ value: JSON.stringify(pacemakerData) }],
    };
    io.emit("pacemakerData", pacemakerData);
 
    try {
      await producer.send(payload);
      console.log("Published:", pacemakerData);
    } catch (err) {
      console.error("Error publishing to Kafka:", err);
    }
  }, 2000);
};
 
// Stop publishing data
const stopPublishing = () => {
  if (!isPublishing) return;
  isPublishing = false;
  clearInterval(publishInterval);
  console.log("Kafka producer stopped");
};
 
// Handle Socket.IO connections
io.on("connection", (socket) => {
  console.log("Client connected");
 
  socket.on("start", () => {
    startPublishing();
  });
 
  socket.on("stop", () => {
    stopPublishing();
  });
 
  socket.on("disconnect", () => {
    console.log("Client disconnected");
    stopPublishing();
  });
});
 
// Start the server
const startServer = async () => {
  try {
    await producer.connect();
    console.log("Kafka Producer is connected and ready.");
    server.listen(8080, () =>
      console.log("Server is running on http://localhost:8080")
    );
  } catch (err) {
    console.error("Failed to start Kafka Producer:", err);
  }
};
 
startServer();