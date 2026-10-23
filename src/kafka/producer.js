// src/kafka/producer.js
const logger = require('../config/logger');

// Kafka is optional. Set KAFKA_ENABLED=false (or leave KAFKA_BROKERS unset)
// on hosts without a broker — bookings still work, only the async
// notification is skipped.
const KAFKA_ENABLED =
  process.env.KAFKA_ENABLED !== 'false' && Boolean(process.env.KAFKA_BROKERS);

let producer = null;
let isConnected = false;
let warnedOnce = false;

async function connectProducer() {
  if (!KAFKA_ENABLED) return null;
  if (!producer) {
    producer = require('../config/kafka').producer;
  }
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
    logger.info('Kafka producer connected');
  }
  return producer;
}

async function sendBookingEvent(booking) {
  if (!KAFKA_ENABLED) {
    if (!warnedOnce) {
      logger.info('Kafka disabled — skipping booking events');
      warnedOnce = true;
    }
    return;
  }

  await connectProducer();

  await producer.send({
    topic: 'booking-events',
    messages: [
      {
        key: String(booking.id),
        value: JSON.stringify({
          eventType: 'BOOKING_CONFIRMED',
          bookingId: booking.id,
          userId: booking.user_id,
          eventId: booking.event_id,
          seatId: booking.seat_id,
          timestamp: new Date().toISOString(),
        }),
      },
    ],
  });

  logger.info({ bookingId: booking.id }, 'booking event published');
}

module.exports = { sendBookingEvent, connectProducer, KAFKA_ENABLED };
