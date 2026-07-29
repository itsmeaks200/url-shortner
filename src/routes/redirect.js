"use strict";

const express = require("express");
const { resolveCode } = require("../services/shortener");
const producer = require("../kafka/producer");

const router = express.Router();

router.get("/:code", async (req, res, next) => {
  try {
    const { code } = req.params;
    const record = await resolveCode(code);

    if (!record) {
      return res.status(404).json({ error: "Short URL not found" });
    }

    if (record.expires_at && new Date() > new Date(record.expires_at)) {
      return res.status(410).json({ error: "This short URL has expired" });
    }

    // Fire-and-forget: emit click event without blocking the redirect.
    // A Kafka failure must never affect redirect latency or reliability.
    producer.emitClickEvent(code, req).catch((err) => {
      console.error("[Analytics] Failed to emit click event:", err.message);
    });

    // Smart caching: Cache up to URL TTL (or use default if no expiry)
    // This maximizes browser cache effectiveness while respecting URL lifetime
    const DEFAULT_CACHE_TTL = parseInt(process.env.BROWSER_CACHE_TTL || '3600', 10); // 1 hour default
    let cacheTTL = DEFAULT_CACHE_TTL;
    
    if (record.expires_at) {
      const secondsUntilExpiry = Math.floor(
        (new Date(record.expires_at) - Date.now()) / 1000
      );
      // Cache for the full TTL remaining (never beyond expiry)
      cacheTTL = Math.max(0, secondsUntilExpiry);
    }

    res.set("Cache-Control", `public, max-age=${cacheTTL}, must-revalidate`);
    // 307 maintains HTTP method and respects Cache-Control
    return res.redirect(307, record.long_url);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
