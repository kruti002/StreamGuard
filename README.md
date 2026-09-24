# StreamGuard — Real-Time Data Quality Firewall on Confluent Cloud

> Catch bad data at the edge, quarantine it with the exact rule it broke, and
> flag a source **before** its errors corrupt the warehouse or your ML features.

Bad data is the silent killer of streaming platforms. A partner changes a field,
a mobile build ships a bug, a batch job double-encodes a timestamp — and
malformed records flow downstream until a dashboard looks wrong or an ML model
drifts days later. By then the bad data is everywhere and the cleanup is
expensive.

**StreamGuard** puts a *firewall* between untrusted producers and trusted
consumers. Every event is checked against a **data contract**. Valid records flow
to a clean lane; violations are **quarantined** to a dead-letter queue with the
exact rules they broke — never silently dropped. In parallel, each source's error
rate is scored live in 10-second windows and turned into an actionable
**OK → WATCH → BREACH** status against a 2% data-quality SLA.

This is a problem streaming is genuinely the right tool for: a nightly batch
quality check only finds the damage after it has already spread downstream.

---

## Demo (what you see live)

A React dashboard shows the firewall working in real time:

- **Source health board** — every upstream source scored live; a degrading
  source (`checkout-svc`) climbs past the 2% SLA into **BREACH** while healthy
  sources stay **OK**.
- **Error-rate chart** — observed error rate vs. a short-term forecast, against
  the 2% SLA line.
- **Dead-letter queue** — a live feed of quarantined records, each tagged with
  the exact contract rule it violated (`INVALID_CURRENCY`, `AMOUNT_OUT_OF_RANGE`,
  `MISSING_USER_ID`, `MALFORMED_TIMESTAMP`, `STALE_EVENT`, `MISSING_EVENT_ID`).

---

## Architecture

```
Feeder (Node)
  └─▶ raw_events  (Kafka topic; JSON Schema registered in Schema Registry)
        └─▶ Flink SQL:  events_checked → events_verdict     (the data contract)
              ├─▶ clean_events        (passed the contract)
              ├─▶ quarantine_events   (failed — kept with the rule that broke)
              └─▶ dq_metrics          (per-source error rate, 10s windows)

Dashboard (Express + React)
  └─▶ consumes raw_events, applies the SAME contract, windows it per source,
      scores OK/WATCH/BREACH + forecast, and streams to the browser over SSE
```

**The contract** (enforced in Flink, mirrored in the dashboard): `event_id` and
`user_id` present, `amount` in `[0, 100000]`, `currency` in a known set,
`event_ts` a parseable timestamp, and freshness within 5 minutes. Schema Registry
guards the *wire format*; the contract guards the *meaning* — which is where real
data-quality incidents live.

See **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** for the full dataflow and design rationale.

## Confluent building blocks used

| Pillar | How StreamGuard uses it |
| --- | --- |
| **Connectors / producers** | A Node producer streams events into `raw_events` (Datagen-style; swappable for real sources). |
| **Stream processing (Flink SQL)** | Contract enforcement, clean/quarantine routing, and per-source windowed quality metrics — continuous SQL. |
| **Stream Governance** | A JSON Schema is registered in Schema Registry and enforced on the ingest topic. |
| **Forecasting** | Per-source error-rate trend projected forward to raise **WATCH** before a source crosses its SLA. |

---

## Repo layout

```
flink/       Flink SQL — run in numeric order in the Confluent SQL Workspace
feeder/      Node producer: valid baseline traffic + escalating contract violations
dashboard/   Express SSE backend + Vite/React client (client/)
docs/        ARCHITECTURE (dataflow + design)
```

## Quick start

Prereqs: a Confluent Cloud account (free trial credit) and Node.js 18+.

1. In Confluent Cloud: create a cluster, enable Schema Registry (Essentials),
   add a `raw_events` topic, and create a Flink compute pool.
2. Run the SQL files in `flink/` in numeric order in the Flink SQL Workspace
   (catalog `default`, database `cluster_0`).
3. Start the feeder — it registers the JSON Schema and streams events:
   ```
   cd feeder && npm install
   cp .env.example .env        # fill in your Kafka + Schema Registry credentials
   npm start
   ```
4. Start the dashboard:
   ```
   cd dashboard && npm run setup
   cp .env.example .env        # fill in your Kafka credentials
   npm start
   ```
   Open **http://localhost:3000**.

> Credentials go only in `.env` (git-ignored). The committed `.env.example`
> files contain placeholders — never real keys.

---

## Implementation notes (honest engineering)

- **Event-time windowing** on the Kafka record time keeps late/out-of-order
  events correct — a degrading source often gets laggy at the same time.
- **Dead-letter queue, not silent drops** — rejected records are preserved with
  their failed rules so they can be triaged and replayed after the upstream fix.
- **Contracts as executable logic** on top of Schema Registry capture semantic
  rules (ranges, enums, freshness) a plain schema can't express.
- The Flink pipeline performs the contract enforcement, quarantine routing, and
  windowed metrics. The dashboard applies the **identical contract** to
  `raw_events` to guarantee a continuously live view for the demo (Confluent
  Cloud's managed Flink ran the forecast-into-a-materialized-table variant as a
  bounded job, so the live view is computed in the serving layer instead). Same
  rules, same windows — computed where they stream reliably.
