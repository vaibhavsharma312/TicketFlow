// src/middleware/metrics.js

const metrics = {
  requests: { total: 0, byStatus: {}, byEndpoint: {} },
  bookings: { pessimistic: { success: 0, conflict: 0 }, optimistic: { success: 0, conflict: 0, retries: 0 } },
  latency: { sum: 0, count: 0, max: 0 },
  startTime: Date.now(),
};

function recordRequest(method, path, status, duration) {
  metrics.requests.total++;
  metrics.requests.byStatus[status] = (metrics.requests.byStatus[status] || 0) + 1;

  const endpoint = `${method} ${path}`;
  metrics.requests.byEndpoint[endpoint] = (metrics.requests.byEndpoint[endpoint] || 0) + 1;

  metrics.latency.sum += duration;
  metrics.latency.count++;
  if (duration > metrics.latency.max) metrics.latency.max = duration;
}

function recordBooking(type, outcome) {
  if (type === 'pessimistic') {
    if (outcome === 'success') metrics.bookings.pessimistic.success++;
    else metrics.bookings.pessimistic.conflict++;
  } else {
    if (outcome === 'success') metrics.bookings.optimistic.success++;
    else if (outcome === 'conflict') metrics.bookings.optimistic.conflict++;
    else if (outcome === 'retry') metrics.bookings.optimistic.retries++;
  }
}

function metricsMiddleware(req, res, next) {
  const start = Date.now();
  res.on('finish', () => {
    recordRequest(req.method, req.route?.path || req.path, res.statusCode, Date.now() - start);
  });
  next();
}

function getMetrics() {
  const uptimeSeconds = Math.round((Date.now() - metrics.startTime) / 1000);
  return {
    uptime: `${uptimeSeconds}s`,
    requests: metrics.requests,
    bookings: metrics.bookings,
    latency: {
      avg: metrics.latency.count > 0 ? Math.round(metrics.latency.sum / metrics.latency.count) + 'ms' : '0ms',
      max: metrics.latency.max + 'ms',
      total: metrics.latency.count,
    },
  };
}

module.exports = { metricsMiddleware, recordBooking, getMetrics };
