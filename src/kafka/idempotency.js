// src/kafka/idempotency.js
const pool = require('../config/db');

async function isAlreadyProcessed(consumerName, messageKey) {
  const result = await pool.query(
    'SELECT id FROM processed_messages WHERE consumer = $1 AND message_key = $2',
    [consumerName, messageKey]
  );
  return result.rows.length > 0;
}

async function markAsProcessed(consumerName, messageKey) {
  try {
    await pool.query(
      'INSERT INTO processed_messages (consumer, message_key) VALUES ($1, $2)',
      [consumerName, messageKey]
    );
  } catch (err) {
    if (err.code === '23505') {
      return false;
    }
    throw err;
  }
  return true;
}

module.exports = { isAlreadyProcessed, markAsProcessed };
