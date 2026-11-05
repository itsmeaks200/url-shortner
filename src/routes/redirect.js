"use strict";

const express = require("express");
const { resolveCode } = require("../services/shortener");
const producer = require("../kafka/producer");

const router = express.Router();

// Generated codes are Base62 and aliases are [a-zA-Z0-9-]; both fit VARCHAR(12).
const CODE_RE = /^[a-zA-Z0-9-]{1,12}$/;

router.get("/:code", async (req, res, next) => {
  try {
    const { code } = req.params;
    // Reject impossible codes (e.g. /favicon.ico) before any Redis/DB work.
    if (!CODE_RE.test(code)) {
      return res.status(404).json({ error: "Short URL not found" });
    }
    const record = await resolveCode(code);

    if (!record) {
      return res.status(404).json({ error: "Short URL not found" });
    }

    if (record.expires_at && new Date() > new Date(record.expires_at)) {
      return res.status(410).json({ error: "This short URL has expired" });
    }

    // Fire-and-forget: emit click event without blocking the redirect.
    // A Kafka failure must never affect redirect latency or reliability.
    // HEAD requests (link previewers, health probes) are not clicks.
    if (req.method !== "HEAD") {
      producer.emitClickEvent(code, req).catch((err) => {
        console.error("[Analytics] Failed to emit click event:", err.message);
      });
    }

    // Smart caching: Cache for 5 minutes (or until expiry if sooner)
    // This balances server load reduction with analytics granularity
    let cacheTTL = 300; // 5 minutes
    
    if (record.expires_at) {
      const secondsUntilExpiry = Math.floor(
        (new Date(record.expires_at) - Date.now()) / 1000
      );
      // Cache for min(5 minutes, time until expiry)
      cacheTTL = Math.min(cacheTTL, Math.max(0, secondsUntilExpiry));
    }

    res.set("Cache-Control", `public, max-age=${cacheTTL}, must-revalidate`);
    return res.redirect(307, record.long_url);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
