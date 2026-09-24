// ============================================================================
// StreamGuard · Bad-Data Injector
// ----------------------------------------------------------------------------
// Streams order/payment-like events from several SOURCES into the Confluent
// Cloud `raw_events` topic. Most sources stay clean (a low steady baseline of
// noise). One source -- the "degrading" one -- gets an ESCALATING rate of
// contract violations over time, so that:
//   * the quarantine (DLQ) topic fills with real, labeled bad records,
//   * dq_metrics shows the error rate climbing per window, and
//   * ML_FORECAST predicts the SLA breach BEFORE it actually happens.
//
// Violations injected (match flink/02_contract_check.sql contract rules):
//   MISSING_USER_ID, AMOUNT_OUT_OF_RANGE, INVALID_CURRENCY,
//   MALFORMED_TIMESTAMP, MISSING_EVENT_ID, STALE_EVENT
//
// Record shape (matches flink/01_raw_events.sql):
//   { event_id, source, amount, currency, user_id, event_ts }  // event_ts ISO-8601
// ============================================================================

import confluentKafka from '@confluentinc/kafka-javascript';
import {
  SchemaRegistryClient,
  JsonSerializer,
  SerdeType,
} from '@confluentinc/schemaregistry';
import { randomUUID } from 'node:crypto';
const { Kafka } = confluentKafka.KafkaJS;
import 'dotenv/config';

const {
  BOOTSTRAP_SERVERS,
  CONFLUENT_API_KEY,
  CONFLUENT_API_SECRET,
  SCHEMA_REGISTRY_URL,
  SCHEMA_REGISTRY_API_KEY,
  SCHEMA_REGISTRY_API_SECRET,
  RAW_TOPIC = 'raw_events',
  EVENTS_PER_TICK = '20',
  TICK_MS = '1000',
  DEGRADE_START_SEC = '90',   // clean baseline before this, then bad-rate ramps
  DEGRADE_RAMP_SEC = '180',   // time to ramp from baseline to peak bad rate
  PEAK_BAD_RATE = '0.35',     // 35% violations at peak on the degrading source
} = process.env;

if (!BOOTSTRAP_SERVERS || !CONFLUENT_API_KEY || !CONFLUENT_API_SECRET) {
  console.error('Missing Kafka config. Copy .env.example to .env and fill it in.');
  process.exit(1);
}
if (!SCHEMA_REGISTRY_URL || !SCHEMA_REGISTRY_API_KEY || !SCHEMA_REGISTRY_API_SECRET) {
  console.error(
    'Missing Schema Registry config. Confluent Cloud Flink needs registry-backed ' +
      'JSON, so set SCHEMA_REGISTRY_URL / _API_KEY / _API_SECRET in .env.'
  );
  process.exit(1);
}

const eventsPerTick = parseInt(EVENTS_PER_TICK, 10);
const tickMs = parseInt(TICK_MS, 10);
const degradeStartMs = parseInt(DEGRADE_START_SEC, 10) * 1000;
const degradeRampMs = parseInt(DEGRADE_RAMP_SEC, 10) * 1000;
const peakBadRate = parseFloat(PEAK_BAD_RATE);

// Sources: one is designated the "degrading" source; the rest stay clean-ish.
const SOURCES = [
  { name: 'checkout-svc', degrading: true,  baseBadRate: 0.01 },
  { name: 'mobile-app',   degrading: false, baseBadRate: 0.005 },
  { name: 'partner-api',  degrading: false, baseBadRate: 0.02 },
  { name: 'batch-import', degrading: false, baseBadRate: 0.01 },
];

const CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'INR'];
const BAD_CURRENCIES = ['usd', 'BTC', 'XYZ', '', 'US'];

// JSON Schema registered for the topic value. It's intentionally PERMISSIVE
// (all fields optional/nullable) because the untrusted edge may send garbage --
// the semantic contract (ranges, enums, freshness) is enforced later in Flink,
// not by this wire schema. This is exactly the shift-left split: Schema Registry
// guards the wire format; Flink guards the meaning.
const VALUE_SCHEMA = {
  type: 'object',
  title: 'raw_event',
  properties: {
    event_id: { type: ['string', 'null'] },
    source: { type: ['string', 'null'] },
    amount: { type: ['number', 'null'] },
    currency: { type: ['string', 'null'] },
    user_id: { type: ['string', 'null'] },
    event_ts: { type: ['string', 'null'] },
  },
  required: [],
  additionalProperties: true,
};

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

const producer = kafka.producer();

// Schema Registry client + JSON Schema serializer. autoRegisterSchemas registers
// VALUE_SCHEMA under the subject `<topic>-value` on first produce, so Flink can
// infer the table and deserialize with the json-registry format.
const registry = new SchemaRegistryClient({
  baseURLs: [SCHEMA_REGISTRY_URL],
  basicAuthCredentials: {
    credentialsSource: 'USER_INFO',
    userInfo: `${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}`,
  },
});

// useLatestVersion:true makes every record serialize against the ONE permissive
// schema we register at startup -- so bad records (nulls, missing fields) still
// serialize on the wire instead of deriving a different schema per message.
// Validation stays OFF here on purpose; semantic checks happen in Flink.
const valueSerializer = new JsonSerializer(registry, SerdeType.VALUE, {
  useLatestVersion: true,
});

// Register the permissive value schema once, up front, under `<topic>-value`.
async function registerValueSchema() {
  const subject = `${RAW_TOPIC}-value`;
  await registry.register(subject, {
    schemaType: 'JSON',
    schema: JSON.stringify(VALUE_SCHEMA),
  });
  console.log(`Registered JSON schema for subject "${subject}".`);
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

// smoothstep for a natural degradation ramp rather than a step change.
function smoothstep(t) {
  const x = Math.max(0, Math.min(1, t));
  return x * x * (3 - 2 * x);
}

// Current probability that a given source emits a bad record.
function badRateFor(source, elapsedMs) {
  if (!source.degrading) return source.baseBadRate;
  if (elapsedMs < degradeStartMs) return source.baseBadRate;
  const t = (elapsedMs - degradeStartMs) / degradeRampMs;
  return source.baseBadRate + smoothstep(t) * (peakBadRate - source.baseBadRate);
}

function validEvent(source) {
  return {
    event_id: `e-${randomUUID().slice(0, 8)}`,
    source: source.name,
    amount: Math.round(Math.random() * 500 * 100) / 100,
    currency: pick(CURRENCIES),
    user_id: `u-${1000 + Math.floor(Math.random() * 9000)}`,
    event_ts: new Date().toISOString(),
  };
}

// Corrupt exactly one field, so each bad record maps to a clear DLQ reason.
function corrupt(evt) {
  const modes = [
    (e) => { e.user_id = null; },                                  // MISSING_USER_ID
    (e) => { e.amount = pick([-50, 250000, 999999]); },            // AMOUNT_OUT_OF_RANGE
    (e) => { e.currency = pick(BAD_CURRENCIES); },                 // INVALID_CURRENCY
    (e) => { e.event_ts = pick(['not-a-date', '13/45/2026', '']); }, // MALFORMED_TIMESTAMP
    (e) => { e.event_id = null; },                                 // MISSING_EVENT_ID
    (e) => { e.event_ts = new Date(Date.now() - 20 * 60 * 1000).toISOString(); }, // STALE_EVENT
  ];
  pick(modes)(evt);
  return evt;
}

async function main() {
  await registerValueSchema();
  await producer.connect();
  console.log(`StreamGuard injector connected. Producing to "${RAW_TOPIC}".`);
  console.log(
    `Degrading source: ${SOURCES.find((s) => s.degrading).name}. ` +
      `Bad-rate ramps from baseline to ${(peakBadRate * 100).toFixed(0)}% ` +
      `starting +${degradeStartMs / 1000}s over ${degradeRampMs / 1000}s.`
  );

  const startedAt = Date.now();

  const tick = async () => {
    const elapsedMs = Date.now() - startedAt;
    const messages = [];
    let badThisTick = 0;

    for (let i = 0; i < eventsPerTick; i++) {
      const source = pick(SOURCES);
      const badRate = badRateFor(source, elapsedMs);
      let evt = validEvent(source);
      if (Math.random() < badRate) {
        evt = corrupt(evt);
        badThisTick++;
      }
      // Serialize with the Schema Registry JSON serializer (magic byte + schema
      // id + JSON payload) so Confluent Flink's json-registry format can read it.
      const value = await valueSerializer.serialize(RAW_TOPIC, evt);
      messages.push({ key: source.name, value });
    }

    try {
      await producer.send({ topic: RAW_TOPIC, messages });
      const degRate = badRateFor(SOURCES.find((s) => s.degrading), elapsedMs);
      process.stdout.write(
        `\r+${String(Math.floor(elapsedMs / 1000)).padStart(4)}s | ` +
          `sent ${eventsPerTick} | bad ${badThisTick} | ` +
          `checkout-svc bad-rate ~${(degRate * 100).toFixed(0)}%   `
      );
    } catch (err) {
      console.error('\nProduce error:', err.message);
    }
  };

  const timer = setInterval(tick, tickMs);
  await tick();

  const shutdown = async () => {
    clearInterval(timer);
    console.log('\nDisconnecting injector...');
    await producer.disconnect();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('Injector failed:', err);
  process.exit(1);
});
