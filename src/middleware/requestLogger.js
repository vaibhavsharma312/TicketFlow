// src/middleware/requestLogger.js
const logger = require('../config/logger');

function requestLogger(req, res, next) {
  const start = Date.now();
  const reqId = req.headers['x-request-id'] || `req-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  req.log = logger.child({ reqId });
  req.log.info({ method: req.method, url: req.originalUrl }, 'request started');

  res.on('finish', () => {
    const duration = Date.now() - start;
    const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';
    req.log[level](
      { method: req.method, url: req.originalUrl, status: res.statusCode, duration: `${duration}ms` },
      'request completed'
    );
  });

  next();
}

module.exports = requestLogger;
