// ============================================================================
// StreamGuard · Dashboard backend
// ----------------------------------------------------------------------------
// Consumes the Flink output topics from Confluent Cloud:
//   dq_alerts          - one message per source per window: status, current &
//                        forecast error rate, minutes-to-SLA-breach, top reason.
//   quarantine_events  - the dead-letter feed: rejected records + failed rules.
//
// It keeps the latest state per source in memory and streams every update to
// the browser over Server-Sent Events (SSE). The frontend renders the health
// board, the forecast chart, and the live DLQ feed.
//
// NOTE ON RESILIENCE: if the topics don't exist yet (pipeline not started), the
// consumer logs a warning and the UI simply shows "waiting for data" instead of
// crashing -- so you can start the dashboard before the Flink jobs are running.
// ============================================================================

import express from 'express';
import confluentKafka from '@confluentinc/kafka-javascript';
const { Kafka } = confluentKafka.KafkaJS;
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import 'dotenv/config';

const __dirname = dirname(fileURLToPath(import.meta.url));

const {
  BOOTSTRAP_SERVERS,
  CONFLUENT_API_KEY,
  CONFLUENT_API_SECRET,
  RAW_TOPIC = 'raw_events',
  ALERTS_TOPIC = 'dq_alerts',
  QUARANTINE_TOPIC = 'quarantine_events',
  PORT = '3000',
  GROUP_ID = 'streamguard-dashboard',
  // If true, the dashboard computes everything itself from raw_events instead
  // of relying on the Flink dq_alerts materialized table. This is the reliable
  // path (Confluent MT with OVER+TUMBLE ran as a bounded/Completed job).
  SELF_COMPUTE = 'true',
} = process.env;

const selfCompute = String(SELF_COMPUTE).toLowerCase() === 'true';

if (!BOOTSTRAP_SERVERS || !CONFLUENT_API_KEY || !CONFLUENT_API_SECRET) {
  console.error('Missing config. Copy .env.example to .env and fill it in.');
  process.exit(1);
}

// ---- In-memory state --------------------------------------------------------
const state = {
  sources: new Map(), // source -> latest alert row
  history: new Map(),  // source -> [{ ts, current, forecast }] (last N points)
  dlq: [],             // recent quarantined records (newest first)
};
const HISTORY_LEN = 40;
const DLQ_LEN = 50;
const SLA_PCT = 2.0;
const WINDOW_MS = 10_000;      // 10s windows, matching the Flink pipeline
const VALID_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'INR'];

// Per-source rolling window accumulator used in self-compute mode.
// windows: Map<source, { windowStart, total, bad, reasons{}, prevErr }>
const windows = new Map();

// The SAME contract StreamGuard enforces in Flink (flink/02_contract_check.sql).
// Returns the array of violated rule codes for one event.
function checkContract(evt) {
  const v = [];
  if (evt.event_id == null || String(evt.event_id).length === 0) v.push('MISSING_EVENT_ID');
  if (evt.user_id == null || String(evt.user_id).length === 0) v.push('MISSING_USER_ID');
  const amt = Number(evt.amount);
  if (evt.amount == null || Number.isNaN(amt) || amt < 0 || amt > 100000) v.push('AMOUNT_OUT_OF_RANGE');
  if (evt.currency == null || !VALID_CURRENCIES.includes(evt.currency)) v.push('INVALID_CURRENCY');
  const t = Date.parse(evt.event_ts);
  if (Number.isNaN(t)) v.push('MALFORMED_TIMESTAMP');
  else if (Date.now() - t > 300_000) v.push('STALE_EVENT');
  return v;
}

// Feed one raw event into the rolling window for its source. When a window
// closes (10s elapsed), finalize it: compute error rate, a next-window forecast
// (linear trend), status, top reason, and push to the source/history state.
function ingestRawEvent(evt) {
  const source = evt.source || 'unknown';
  const now = Date.now();
  let w = windows.get(source);
  if (!w) {
    w = { start: now, total: 0, bad: 0, reasons: {}, prevErr: 0 };
    windows.set(source, w);
  }

  // roll the window if 10s elapsed
  if (now - w.start >= WINDOW_MS && w.total > 0) {
    finalizeWindow(source, w);
    w = { start: now, total: 0, bad: 0, reasons: {}, prevErr: w.lastErr ?? 0 };
    windows.set(source, w);
  }

  const violations = checkContract(evt);
  w.total += 1;
  if (violations.length) {
    w.bad += 1;
    for (const r of violations) w.reasons[r] = (w.reasons[r] || 0) + 1;
    // record to the DLQ feed
    state.dlq.unshift({ source, event_id: evt.event_id ?? null, violations, at: new Date().toISOString() });
    while (state.dlq.length > DLQ_LEN) state.dlq.pop();
  }
}

function finalizeWindow(source, w) {
  const current = w.total ? Math.round((100 * w.bad / w.total) * 100) / 100 : 0;
  w.lastErr = current;

  // Forecast = short-term trend on the recent observed points, damped and
  // clamped to [0,100] so a couple of noisy windows can't send it to 190%.
  const recentHist = state.history.get(source) ?? [];
  const recent = recentHist.slice(-4).map((p) => p.current);
  recent.push(current);
  const avg = recent.reduce((a, b) => a + b, 0) / recent.length;
  const slope = recent.length >= 2 ? recent[recent.length - 1] - recent[0] : 0;
  // next-window estimate: average nudged by a damped trend
  let forecast = avg + slope * 0.5;
  forecast = Math.max(0, Math.min(100, Math.round(forecast * 100) / 100));

  const status = current >= SLA_PCT ? 'BREACH' : forecast >= SLA_PCT ? 'WATCH' : 'OK';
  let topReason = 'NONE';
  let max = 0;
  for (const [k, n] of Object.entries(w.reasons)) {
    if (n > max) { max = n; topReason = k; }
  }

  const ts = new Date().toISOString();
  const minutesToBreach =
    current >= SLA_PCT ? 0 :
    forecast > current ? ((SLA_PCT - current) / (forecast - current)) * (WINDOW_MS / 1000 / 60) :
    null;

  state.sources.set(source, {
    source, ts,
    total_events: w.total,
    current_error_pct: current,
    forecast_error_pct: forecast,
    forecast_upper_pct: forecast,
    minutes_to_sla_breach: minutesToBreach,
    status,
    top_reason: topReason,
  });

  const hist = state.history.get(source) ?? [];
  hist.push({ ts, current, forecast, upper: forecast });
  while (hist.length > HISTORY_LEN) hist.shift();
  state.history.set(source, hist);
}

// ---- SSE plumbing -----------------------------------------------------------
const clients = new Set();

function broadcast() {
  const payload = JSON.stringify(snapshot());
  for (const res of clients) {
    res.write(`data: ${payload}\n\n`);
  }
}

function snapshot() {
  return {
    sources: [...state.sources.values()].sort(statusOrder),
    history: Object.fromEntries(state.history),
    dlq: state.dlq.slice(0, DLQ_LEN),
    updatedAt: new Date().toISOString(),
  };
}

function statusOrder(a, b) {
  const rank = { BREACH: 0, WATCH: 1, OK: 2 };
  const d = (rank[a.status] ?? 3) - (rank[b.status] ?? 3);
  if (d !== 0) return d;
  return (a.minutes_to_sla_breach ?? Infinity) - (b.minutes_to_sla_breach ?? Infinity);
}

// ---- Kafka consumer ---------------------------------------------------------
const kafka = new Kafka({
  kafkaJS: {
    brokers: [BOOTSTRAP_SERVERS],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: CONFLUENT_API_KEY,
      password: CONFLUENT_API_SECRET,
    },
  },
});

// Flink writes these topics with the Schema Registry JSON (JSON_SR) format:
// a 1-byte magic (0x00) + 4-byte schema id prefix, THEN the UTF-8 JSON payload.
// So a plain JSON.parse fails. We try plain first (in case a topic is raw JSON),
// then fall back to stripping the 5-byte wire prefix and parsing from the JSON.
function safeParse(buf) {
  if (!buf) return null;
  // 1) plain JSON
  try {
    return JSON.parse(buf.toString('utf8'));
  } catch {
    /* fall through */
  }
  // 2) Confluent wire format: skip 5-byte prefix, parse from first '{' or '['
  try {
    const s = buf.toString('utf8');
    const start = s.search(/[[{]/);
    if (start >= 0) return JSON.parse(s.slice(start));
  } catch {
    /* fall through */
  }
  return null;
}

function handleAlert(msg) {
  const row = safeParse(msg.value);
  if (!row || !row.source) return;
  state.sources.set(row.source, row);

  const hist = state.history.get(row.source) ?? [];
  hist.push({
    ts: row.ts ?? new Date().toISOString(),
    current: Number(row.current_error_pct ?? 0),
    forecast: Number(row.forecast_error_pct ?? 0),
    upper: Number(row.forecast_upper_pct ?? 0),
  });
  while (hist.length > HISTORY_LEN) hist.shift();
  state.history.set(row.source, hist);
}

function handleQuarantine(msg) {
  const row = safeParse(msg.value);
  if (!row) return;
  state.dlq.unshift({
    source: row.source,
    event_id: row.event_id,
    violations: row.violations ?? [],
    at: row.quarantined_at ?? new Date().toISOString(),
  });
  while (state.dlq.length > DLQ_LEN) state.dlq.pop();
}

async function startConsumer() {
  const consumer = kafka.consumer({
    // fromBeginning: true so the dashboard picks up alerts already produced
    // before it started (the pipeline warms up for ~100s before emitting).
    kafkaJS: { groupId: GROUP_ID, fromBeginning: true },
  });
  await consumer.connect();

  // In self-compute mode we read raw_events and do the contract + windowing in
  // Node (reliable, no dependency on the Flink dq_alerts materialized table).
  // Otherwise we read the Flink output topics dq_alerts + quarantine_events.
  const topics = selfCompute
    ? [RAW_TOPIC]
    : [ALERTS_TOPIC, QUARANTINE_TOPIC];

  for (const topic of topics) {
    try {
      await consumer.subscribe({ topic });
      console.log(`Subscribed to "${topic}".`);
    } catch (err) {
      console.warn(`Could NOT subscribe to "${topic}": ${err.message}`);
    }
  }
  console.log(selfCompute
    ? 'Mode: SELF_COMPUTE (contract + windows computed in the dashboard from raw_events)'
    : 'Mode: Flink topics (dq_alerts + quarantine_events)');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (selfCompute && topic === RAW_TOPIC) {
        const evt = safeParse(message.value);
        if (evt) ingestRawEvent(evt);
      } else if (topic === ALERTS_TOPIC) {
        handleAlert(message);
      } else if (topic === QUARANTINE_TOPIC) {
        handleQuarantine(message);
      }
      broadcast();
    },
  });

  return consumer;
}

// In self-compute mode, also flush/finalize windows on a timer so sources that
// go quiet still close their window and the UI keeps updating.
if (selfCompute) {
  setInterval(() => {
    const now = Date.now();
    let changed = false;
    for (const [source, w] of windows.entries()) {
      if (w.total > 0 && now - w.start >= WINDOW_MS) {
        finalizeWindow(source, w);
        windows.set(source, { start: now, total: 0, bad: 0, reasons: {}, prevErr: w.lastErr ?? 0 });
        changed = true;
      }
    }
    if (changed) broadcast();
  }, 2000);
}

// ---- HTTP + SSE -------------------------------------------------------------
const app = express();

// Serve the built React app (run `npm run build` in client/ first). In dev you
// can instead run the Vite dev server on :5173, which proxies /api to here.
const clientDist = join(__dirname, 'client', 'dist');
app.use(express.static(clientDist));

app.get('/api/state', (_req, res) => res.json(snapshot()));

app.get('/api/stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.flushHeaders();
  res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
  clients.add(res);
  req.on('close', () => clients.delete(res));
});

// SPA fallback: any non-API route serves the React index.html.
app.get(/^\/(?!api\/).*/, (_req, res) => {
  res.sendFile(join(clientDist, 'index.html'), (err) => {
    if (err) {
      res
        .status(200)
        .send('StreamGuard: build the client first — run `npm run build` in dashboard/client.');
    }
  });
});

app.listen(parseInt(PORT, 10), () => {
  console.log(`StreamGuard dashboard on http://localhost:${PORT}`);
});

startConsumer().catch((err) => {
  console.error('Consumer failed to start:', err.message);
  // keep the web server up so the UI can still load and show "waiting for data"
});
