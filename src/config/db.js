// src/config/db.js
const { Pool } = require('pg');

// Managed Postgres providers require TLS; local Docker does not.
// PGSSL=true forces it on, otherwise we sniff the host.
function poolConfig(connectionString) {
  const config = {
    connectionString,
    max: parseInt(process.env.PG_POOL_MAX, 10) || 20,
  };

  const needsSsl =
    process.env.PGSSL === 'true' ||
    /render\.com|neon\.tech|supabase\.co|amazonaws\.com/.test(connectionString || '');

  if (needsSsl) {
    // Managed providers terminate TLS with their own CA chain.
    config.ssl = { rejectUnauthorized: false };
  }

  return config;
}

// PRIMARY — all writes, and any read that must be fresh.
const primaryPool = new Pool(poolConfig(process.env.DATABASE_URL));

// REPLICA — read-only queries. Falls back to the primary when no replica is
// configured, so the app runs unchanged on hosts that don't offer one.
const hasReplica = Boolean(process.env.DATABASE_URL_REPLICA);

const replicaPool = hasReplica
  ? new Pool(poolConfig(process.env.DATABASE_URL_REPLICA))
  : primaryPool;

module.exports = primaryPool;
module.exports.primary = primaryPool;
module.exports.replica = replicaPool;
module.exports.hasReplica = hasReplica;
