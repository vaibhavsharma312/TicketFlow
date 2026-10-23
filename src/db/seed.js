// src/db/seed.js
require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const EVENTS = [
  {
    name: 'Coldplay: Music of the Spheres',
    venue: 'Wankhede Stadium, Mumbai',
    event_time: '2026-09-01T19:00:00Z',
    description: 'Experience the magic of Coldplay live with their spectacular Music of the Spheres World Tour. LED wristbands, confetti, and hits spanning two decades.',
    category: 'concert',
    price: 4999,
    image_url: 'https://images.unsplash.com/photo-1540039155733-5bb30b53aa14?w=800',
    seats: { rows: ['A', 'B'], perRow: 8 },
  },
  {
    name: 'IPL Final 2026',
    venue: 'Narendra Modi Stadium, Ahmedabad',
    event_time: '2026-10-15T19:30:00Z',
    description: 'The biggest night in cricket! Watch the IPL 2026 Final live with 130,000 fans under the floodlights.',
    category: 'sports',
    price: 2999,
    image_url: 'https://images.unsplash.com/photo-1531415074968-036ba1b575da?w=800',
    seats: { rows: ['A', 'B'], perRow: 10 },
  },
  {
    name: 'Arijit Singh Live',
    venue: 'Jawaharlal Nehru Stadium, Delhi',
    event_time: '2026-08-20T18:30:00Z',
    description: 'India\'s most loved voice performs his greatest hits — Tum Hi Ho, Channa Mereya, Kesariya and more in an intimate open-air setting.',
    category: 'concert',
    price: 1999,
    image_url: 'https://images.unsplash.com/photo-1493225457124-a3eb161ffa5f?w=800',
    seats: { rows: ['A'], perRow: 10 },
  },
  {
    name: 'Stand-Up Comedy Night',
    venue: 'Canvas Laugh Club, Mumbai',
    event_time: '2026-07-25T20:00:00Z',
    description: 'An evening of non-stop laughter featuring top Indian comedians. Limited seats for an exclusive show.',
    category: 'comedy',
    price: 799,
    image_url: 'https://images.unsplash.com/photo-1585699324551-f6c309eedeca?w=800',
    seats: { rows: ['A'], perRow: 6 },
  },
];

const USERS = [
  'tushar@ticketflow.dev',
  'demo@ticketflow.dev',
  'test@ticketflow.dev',
];

async function seed() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      'TRUNCATE bookings, seats, events, users, processed_messages RESTART IDENTITY CASCADE'
    );

    // Create users
    for (const email of USERS) {
      await client.query('INSERT INTO users (email) VALUES ($1)', [email]);
    }

    // Create events with seats
    for (const evt of EVENTS) {
      const eventResult = await client.query(
        `INSERT INTO events (name, venue, event_time, description, category, price, image_url, total_seats)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
        [evt.name, evt.venue, evt.event_time, evt.description, evt.category, evt.price, evt.image_url,
         evt.seats.rows.length * evt.seats.perRow]
      );
      const eventId = eventResult.rows[0].id;

      for (const row of evt.seats.rows) {
        for (let i = 1; i <= evt.seats.perRow; i++) {
          await client.query(
            'INSERT INTO seats (event_id, seat_number, price) VALUES ($1, $2, $3)',
            [eventId, `${row}${i}`, evt.price]
          );
        }
      }
    }

    await client.query('COMMIT');
    console.log(`Seeded ${USERS.length} users, ${EVENTS.length} events with seats.`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

seed().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
