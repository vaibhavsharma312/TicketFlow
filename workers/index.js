// workers/index.js
require('dotenv').config();

const { consumer } = require('../src/config/kafka');
const { isAlreadyProcessed, markAsProcessed } = require('../src/kafka/idempotency');
const { processBookingConfirmed } = require('./consumers/bookingProcessor');

const TOPIC = 'booking-events';
const CONSUMER_NAME = 'booking-worker';

async function startWorker() {
  console.log('Connecting consumer...');
  await consumer.connect();
  console.log('Consumer connected');

  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
  console.log(`Subscribed to topic: ${TOPIC}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const value = JSON.parse(message.value.toString());
      const messageId = `${topic}-${partition}-${message.offset}`;

      console.log('──────────────────────────────────────');
      console.log(`Received: ${value.eventType} | Booking #${value.bookingId} | Key: ${messageId}`);

      const alreadyDone = await isAlreadyProcessed(CONSUMER_NAME, messageId);
      if (alreadyDone) {
        console.log(`  ⚡ DUPLICATE — already processed, skipping`);
        console.log('──────────────────────────────────────');
        return;
      }

      if (value.eventType === 'BOOKING_CONFIRMED') {
        await processBookingConfirmed(value);
      } else {
        console.log(`  Unknown event type: ${value.eventType}, skipping`);
      }

      await markAsProcessed(CONSUMER_NAME, messageId);
      console.log(`  ✅ Marked as processed: ${messageId}`);
      console.log('──────────────────────────────────────');
    },
  });

  console.log('Worker is running, waiting for messages...');
}

startWorker().catch((err) => {
  console.error('Worker failed:', err);
  process.exit(1);
});
