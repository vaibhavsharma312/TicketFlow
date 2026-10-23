// src/routes/holdRoutes.js
const { Router } = require('express');
const holdService = require('../services/holdService');
const queueService = require('../services/queueService');

const router = Router();

// Phase 1 — hold seats and get a payment id
router.post('/hold', queueService.requireQueueToken, async (req, res, next) => {
  try {
    const { userId, eventId, seatIds, idempotencyKey } = req.body;

    if (!userId || !eventId || !Array.isArray(seatIds) || seatIds.length === 0) {
      return res.status(400).json({ error: 'userId, eventId and a non-empty seatIds array are required' });
    }
    if (seatIds.length > 10) {
      return res.status(400).json({ error: 'Cannot hold more than 10 seats at once' });
    }

    const result = await holdService.holdSeats(userId, eventId, seatIds, idempotencyKey);

    if (result.error) {
      return res.status(result.status).json({ error: result.error, conflictSeat: result.conflictSeat });
    }
    res.status(result.alreadyExisted ? 200 : 201).json(result);
  } catch (err) { next(err); }
});

// Phase 2 — payment gateway callback (simulated). Idempotent.
router.post('/payment/confirm', async (req, res, next) => {
  try {
    const { paymentId, success = true, queueToken } = req.body;
    if (!paymentId) return res.status(400).json({ error: 'paymentId is required' });

    const result = await holdService.confirmPayment(paymentId, success !== false);

    if (queueToken) await queueService.release(queueToken);

    if (result.error) return res.status(result.status).json({ error: result.error });
    res.status(200).json(result);
  } catch (err) { next(err); }
});

// Poll a hold (countdown timer in the UI)
router.get('/hold/:paymentId', async (req, res, next) => {
  try {
    const status = await holdService.getHoldStatus(req.params.paymentId);
    if (!status) return res.status(404).json({ error: 'Unknown payment id' });
    res.json(status);
  } catch (err) { next(err); }
});

// ── Virtual waiting room ──
router.post('/queue/join', async (req, res, next) => {
  try { res.json(await queueService.join()); } catch (err) { next(err); }
});

router.get('/queue/status', async (req, res, next) => {
  try {
    const token = req.query.token;
    if (!token) return res.status(400).json({ error: 'token is required' });
    res.json(await queueService.getStatus(token));
  } catch (err) { next(err); }
});

router.get('/queue/stats', async (req, res, next) => {
  try { res.json(await queueService.stats()); } catch (err) { next(err); }
});

module.exports = router;
