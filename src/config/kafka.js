// src/config/kafka.js
const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'ticketflow',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:29092').split(','),
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'ticketflow-workers' });

module.exports = { kafka, producer, consumer };
