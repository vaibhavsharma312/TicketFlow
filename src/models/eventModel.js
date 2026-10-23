// src/models/eventModel.js
const { primary, replica } = require('../config/db');

// Reads default to the replica. Pass fresh=true to force the primary
// (read-your-writes: right after a booking, the replica may lag).
function pick(fresh) {
  return fresh ? primary : replica;
}

async function getAllEvents(fresh = false) {
  const result = await pick(fresh).query(
    `SELECT e.id, e.name, e.venue, e.event_time, e.description, e.category, e.price, e.image_url, e.total_seats,
            COUNT(CASE WHEN s.status = 'available' THEN 1 END)::int AS available_seats,
            COUNT(CASE WHEN s.status = 'held'      THEN 1 END)::int AS held_seats,
            COUNT(CASE WHEN s.status = 'booked'    THEN 1 END)::int AS booked_seats
     FROM events e
     LEFT JOIN seats s ON s.event_id = e.id
     GROUP BY e.id
     ORDER BY e.event_time`
  );
  return result.rows;
}

async function getEventById(eventId, fresh = false) {
  const result = await pick(fresh).query(
    `SELECT id, name, venue, event_time, description, category, price, image_url, total_seats, created_at
     FROM events WHERE id = $1`,
    [eventId]
  );
  return result.rows[0] || null;
}

module.exports = { getAllEvents, getEventById };
