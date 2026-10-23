// src/models/seatModel.js
const { primary, replica } = require('../config/db');

async function getSeatsByEventId(eventId, fresh = false) {
  const pool = fresh ? primary : replica;
  const result = await pool.query(
    `SELECT id, seat_number, status, version, price
     FROM seats
     WHERE event_id = $1
     ORDER BY seat_number`,
    [eventId]
  );
  return result.rows;
}

module.exports = { getSeatsByEventId };
