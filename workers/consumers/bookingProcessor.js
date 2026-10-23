// workers/consumers/bookingProcessor.js
const pool = require('../../src/config/db');

async function processBookingConfirmed(data) {
  console.log(`  [Processor] Processing booking #${data.bookingId}...`);

  const result = await pool.query(
    'SELECT id, status FROM bookings WHERE id = $1',
    [data.bookingId]
  );

  if (!result.rows[0]) {
    console.log(`  [Processor] Booking #${data.bookingId} not found — skipping`);
    return;
  }

  const booking = result.rows[0];
  console.log(`  [Processor] Booking #${data.bookingId} status: ${booking.status}`);

  // Simulate email
  await new Promise(resolve => setTimeout(resolve, 100));
  console.log(`  [Processor] ✉️  Confirmation email sent for booking #${data.bookingId}`);

  // Simulate analytics
  console.log(`  [Processor] 📊 Analytics updated for event #${data.eventId}`);
}

module.exports = { processBookingConfirmed };
