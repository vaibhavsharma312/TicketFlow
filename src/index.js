// src/index.js
require('dotenv').config();

const path = require('path');
const express = require('express');
const eventRoutes = require('./routes/eventRoutes');
const bookingRoutes = require('./routes/bookingRoutes');
const holdRoutes = require('./routes/holdRoutes');
const errorHandler = require('./middleware/errorHandler');
const requestLogger = require('./middleware/requestLogger');
const { metricsMiddleware, getMetrics } = require('./middleware/metrics');
const logger = require('./config/logger');
const { primary, replica, hasReplica } = require('./config/db');
const cache = require('./config/cache');
const expireHolds = require('./jobs/expireHolds');
const queueService = require('./services/queueService');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(requestLogger);
app.use(metricsMiddleware);
app.use(express.static(path.join(__dirname, '..', 'public')));

app.get('/health', (req, res) => {
  res.json({ status: 'ok', replica: hasReplica, timestamp: new Date().toISOString() });
});

app.get('/metrics', (req, res) => {
  res.json(getMetrics());
});

// ─── Replication status: shows lag between primary and replica ───
app.get('/api/replication-status', async (req, res, next) => {
  try {
    if (!hasReplica) {
      return res.json({ enabled: false, message: 'No replica configured — reads served by primary' });
    }

    const [primaryInfo, replicaInfo] = await Promise.all([
      primary.query('SELECT pg_current_wal_lsn() AS lsn'),
      replica.query(`SELECT pg_last_wal_replay_lsn() AS lsn,
                            EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) AS lag_seconds,
                            pg_is_in_recovery() AS is_standby`),
    ]);

    const lag = replicaInfo.rows[0].lag_seconds;

    res.json({
      enabled: true,
      primaryLsn: primaryInfo.rows[0].lsn,
      replicaLsn: replicaInfo.rows[0].lsn,
      replicaIsStandby: replicaInfo.rows[0].is_standby,
      lagSeconds: lag === null ? 0 : Number(Number(lag).toFixed(3)),
      inSync: primaryInfo.rows[0].lsn === replicaInfo.rows[0].lsn,
    });
  } catch (err) {
    next(err);
  }
});

// ─── User bookings (read — served by replica unless ?fresh=1) ───
app.get('/api/bookings', async (req, res, next) => {
  try {
    const fresh = req.query.fresh === '1';
    const pool = fresh ? primary : replica;
    const userId = parseInt(req.query.userId) || 1;

    const result = await pool.query(
      `SELECT b.id, b.status, b.created_at,
              e.name AS event_name, e.venue, e.event_time,
              s.seat_number, s.price
       FROM bookings b
       JOIN events e ON e.id = b.event_id
       JOIN seats s ON s.id = b.seat_id
       WHERE b.user_id = $1
       ORDER BY b.created_at DESC`,
      [userId]
    );

    res.set('X-Read-Source', fresh || !hasReplica ? 'primary' : 'replica');
    res.json(result.rows);
  } catch (err) { next(err); }
});

// ─── Admin reset ───
app.post('/api/admin/reset', async (req, res) => {
  const { exec } = require('child_process');
  exec('npm run seed', { cwd: path.join(__dirname, '..') }, (err, stdout) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ status: 'reset', message: stdout.trim() });
  });
});

app.use('/api', eventRoutes);
app.use('/api', bookingRoutes);
app.use('/api', holdRoutes);
app.use(errorHandler);

(async () => {
  await cache.init();
  expireHolds.start();
})();

app.listen(PORT, '0.0.0.0', () => {
  logger.info({ port: PORT, replica: hasReplica, cache: cache.getMode(), queue: queueService.isEnabled() }, 'TicketFlow API running');
  console.log('  Reads: ' + (hasReplica ? 'REPLICA (:5434)' : 'primary') + ' + ' + cache.getMode() + ' cache   |   Writes: PRIMARY (:5433)');
  console.log('  Two-phase booking: POST /api/hold -> POST /api/payment/confirm');
  console.log('  Virtual queue: ' + (queueService.isEnabled() ? 'ENABLED' : 'disabled (set QUEUE_ENABLED=true)'));
  console.log(`\n  🎬 Open in browser: http://localhost:${PORT}`);
  });
