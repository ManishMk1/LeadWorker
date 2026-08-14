# Revolt Platform — Implementation Reference

This document is the single source of truth for architecture decisions, conventions, and patterns across all projects in the Revolt customer journey platform. Every project must follow this reference to ensure consistency.

---

## 1. Platform Overview

```
REVOLT WEBSITE
     │
     ▼
revolt-analytics-sdk       → Captures user events from browser
     │
     ▼
event-service              → Receives, validates, enriches, publishes to Kafka
     │
     ▼
     Kafka (webevents.v1.json + other topics)
     │
     ├──► LeadWorkers (this project)   → Writes raw events to ClickHouse
     │
     ├──► customer-service             → Builds customer profiles + identity resolution
     │
     ├──► scoring-service              → Points engine + segment evaluation
     │
     └──► campaign-engine              → Temporal workflows (journey execution)
                │
                ▼
        communication-service          → WhatsApp / Email / SMS dispatch
                │
                ▼
        Meta / Pinbot / Email Provider
```

---

## 2. Projects in This Platform

| # | Project | Language | Primary Role |
|---|---------|----------|--------------|
| 1 | `revolt-analytics-sdk` | TypeScript (browser) | Capture + send events from website |
| 2 | `event-service` | Node.js / TypeScript | HTTP API to receive events, publish to Kafka |
| 3 | `LeadWorkers` (this) | Node.js / JS | Kafka consumer → ClickHouse raw event store |
| 4 | `customer-service` | Node.js / TypeScript | Customer profiles, identity resolution, event history |
| 5 | `scoring-service` | Node.js / TypeScript | Scoring rules, user scores, segment membership |
| 6 | `campaign-builder` | React + TypeScript | React Flow UI for building journeys |
| 7 | `campaign-engine` | Node.js / TypeScript + Temporal | Execute journeys durably via Temporal |
| 8 | `communication-service` | Node.js / TypeScript | WhatsApp / Email / SMS abstraction layer |

---

## 3. Tech Stack (All Projects)

| Concern | Choice | Notes |
|---------|--------|-------|
| Runtime | Node.js ≥ 18 | ESM modules (`"type": "module"`) |
| Language | JavaScript (workers) / TypeScript (services) | TS for services with business logic |
| Message Bus | Kafka (KafkaJS 2.x) | SSL enabled in production |
| Primary DB | MySQL 8 (mysql2/promise) | Leads, customers, campaigns, rules |
| Analytics DB | ClickHouse | Raw event store, reporting |
| Workflow Engine | Temporal (TypeScript SDK) | campaign-engine only |
| Logger | Winston + DailyRotateFile | Structured JSON in production |
| Config | dotenv + central config object | Never read `process.env` directly outside config |
| Container | Docker | All services containerised |

---

## 4. Project Structure Convention

Every project must follow this layout:

```
<project-name>/
├── src/
│   ├── config/
│   │   └── index.ts        ← All env vars resolved here
│   ├── lib/
│   │   ├── kafka.ts        ← Kafka singleton (consumer/producer/admin)
│   │   ├── db.ts           ← MySQL pool singleton
│   │   ├── clickhouse.ts   ← ClickHouse client singleton (if needed)
│   │   └── logger.ts       ← Winston logger singleton
│   ├── workers/            ← Kafka consumers (LeadWorkers pattern)
│   │   └── index.ts
│   ├── routes/             ← HTTP routes (event-service, customer-service, etc.)
│   │   └── index.ts
│   ├── activities/         ← Temporal activities (campaign-engine only)
│   ├── workflows/          ← Temporal workflows (campaign-engine only)
│   └── index.ts            ← Entry point
├── .env.example            ← Required env vars (no secrets committed)
├── .env                    ← Local secrets (gitignored)
├── .gitignore
├── package.json
├── tsconfig.json
└── Dockerfile
```

---

## 5. Config Pattern

Every project has `src/config/index.ts` (or `.js`). **All `process.env` reads happen only here.**

```typescript
// src/config/index.ts
import 'dotenv/config';

const config = {
  env: process.env.NODE_ENV || 'development',

  kafka: {
    clientId:       process.env.KAFKA_CLIENT_ID        || 'revolt-service-name',
    brokers:       (process.env.KAFKA_BROKERS          || 'localhost:9092').split(',').map(b => b.trim()),
    ssl: {
      rejectUnauthorized: process.env.KAFKA_SSL_REJECT_UNAUTHORIZED === 'true',
      caFile:   process.env.KAFKA_SSL_CA_FILE   || 'ca.crt',
      keyFile:  process.env.KAFKA_SSL_KEY_FILE  || 'client.key',
      certFile: process.env.KAFKA_SSL_CERT_FILE || 'signed-certificate-form-acm.pem',
    },
  },

  mysql: {
    host:            process.env.MYSQL_HOST             || 'localhost',
    port:      parseInt(process.env.MYSQL_PORT, 10)     || 3306,
    user:            process.env.MYSQL_USER             || 'root',
    password:        process.env.MYSQL_PASSWORD         || '',
    database:        process.env.MYSQL_DATABASE         || 'revolt',
    connectionLimit: parseInt(process.env.MYSQL_CONNECTION_LIMIT, 10) || 10,
  },

  clickhouse: {
    url:        process.env.CLICKHOUSE_URL       || 'http://localhost:8123',
    database:   process.env.CLICKHOUSE_DATABASE  || 'analytics',
    username:   process.env.CLICKHOUSE_USERNAME  || 'default',
    password:   process.env.CLICKHOUSE_PASSWORD  || '',
    requestTimeout:     parseInt(process.env.CLICKHOUSE_REQUEST_TIMEOUT, 10)  || 30000,
    maxOpenConnections: parseInt(process.env.CLICKHOUSE_MAX_CONNECTIONS, 10)  || 10,
  },

  temporal: {
    address:   process.env.TEMPORAL_ADDRESS   || 'localhost:7233',
    namespace: process.env.TEMPORAL_NAMESPACE || 'default',
    taskQueue: process.env.TEMPORAL_TASK_QUEUE || 'revolt-campaign-queue',
  },

  logging: {
    level: process.env.LOG_LEVEL || 'info',
    dir:   process.env.LOG_DIR   || 'logs',
  },
};

export default config;
```

### .env.example (commit this, never .env)

```bash
NODE_ENV=development

# Kafka
KAFKA_CLIENT_ID=revolt-service-name
KAFKA_BROKERS=localhost:9092
KAFKA_SSL_REJECT_UNAUTHORIZED=false
KAFKA_SSL_CA_FILE=ca.crt
KAFKA_SSL_KEY_FILE=client.key
KAFKA_SSL_CERT_FILE=signed-certificate-form-acm.pem

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=revolt
MYSQL_CONNECTION_LIMIT=10

# ClickHouse
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_DATABASE=analytics
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=

# Temporal (campaign-engine only)
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=revolt-campaign-queue

# Logging
LOG_LEVEL=info
LOG_DIR=logs
```

---

## 6. Kafka Patterns

### Singleton client (copy this into every project)

```typescript
// src/lib/kafka.ts
import { Kafka, logLevel } from 'kafkajs';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import config from '../config/index.js';
import logger from './logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
const CERT_DIR   = path.resolve(__dirname, '../certificates');

let kafkaInstance: Kafka | null = null;

export function getKafkaClient(): Kafka {
  if (kafkaInstance) return kafkaInstance;

  kafkaInstance = new Kafka({
    clientId: config.kafka.clientId,
    brokers:  config.kafka.brokers,
    ssl: {
      rejectUnauthorized: config.kafka.ssl.rejectUnauthorized,
      ca:   [fs.readFileSync(path.join(CERT_DIR, config.kafka.ssl.caFile),   'utf-8')],
      key:   fs.readFileSync(path.join(CERT_DIR, config.kafka.ssl.keyFile),  'utf-8'),
      cert:  fs.readFileSync(path.join(CERT_DIR, config.kafka.ssl.certFile), 'utf-8'),
    },
    logLevel: logLevel.INFO,
    retry: {
      initialRetryTime: 300,
      retries:          10,
      maxRetryTime:     30000,
      factor:           2,
    },
  });

  return kafkaInstance;
}

export function createConsumer(groupId: string, options = {}) {
  return getKafkaClient().consumer({
    groupId,
    sessionTimeout:      30000,
    heartbeatInterval:   3000,
    maxBytesPerPartition: 1048576,
    ...options,
  });
}

export function createProducer(options = {}) {
  return getKafkaClient().producer({
    allowAutoTopicCreation: false,
    transactionTimeout: 30000,
    ...options,
  });
}

export function createAdmin() {
  return getKafkaClient().admin();
}

export async function healthCheck() {
  const t = Date.now();
  try {
    const admin = createAdmin();
    await admin.connect();
    await admin.listTopics();
    await admin.disconnect();
    return { success: true, durationMs: Date.now() - t };
  } catch (error) {
    return { success: false, durationMs: Date.now() - t, error: (error as Error).message };
  }
}
```

### Kafka Topics

| Topic | Producer | Consumers | Purpose |
|-------|----------|-----------|---------|
| `webevents.v1.json` | event-service | LeadWorkers, scoring-service | Raw website events |
| `customer.events.v1` | customer-service | scoring-service, campaign-engine | Customer profile changes |
| `scoring.updates.v1` | scoring-service | campaign-engine | Score + segment changes |
| `campaign.signals.v1` | communication-service, webhooks | campaign-engine | External signals (reply, click, etc.) |
| `notifications.v1` | campaign-engine | communication-service | Outbound message requests |

### Consumer group IDs

Format: `revolt-<service>-<role>-group`

Examples:
- `revolt-leadworkers-events-group`
- `revolt-scoring-events-group`
- `revolt-campaign-engine-signals-group`

---

## 7. Logger Pattern

Copy `src/lib/logger.ts` from LeadWorkers into every project. Change only the `service` label:

```typescript
info.service = 'revolt-<project-name>';
```

Always log with structured metadata:

```typescript
// GOOD
logger.info('Customer created', { component: 'customer-service', customerId, phone });
logger.error('Kafka insert failed', { component: 'clickhouse', error: err.message, stack: err.stack });

// BAD
logger.info('Customer created ' + customerId);
console.log('error:', err);
```

Log levels:
- `error` — something failed, needs attention
- `warn` — unexpected but handled
- `info` — lifecycle events (start, stop, connect, batch inserted)
- `debug` — per-message or per-row tracing (dev only)

---

## 8. MySQL Pattern

```typescript
// src/lib/db.ts
import mysql from 'mysql2/promise';
import config from '../config/index.js';
import logger from './logger.js';

const pool = mysql.createPool({
  host:            config.mysql.host,
  port:            config.mysql.port,
  user:            config.mysql.user,
  password:        config.mysql.password,
  database:        config.mysql.database,
  connectionLimit: config.mysql.connectionLimit,
  waitForConnections: true,
  queueLimit: 0,
});

export async function query<T = any>(sql: string, params?: any[]): Promise<T> {
  const [rows] = await pool.execute(sql, params);
  return rows as T;
}

export async function getConnection() {
  return pool.getConnection();
}

export async function healthCheck() {
  try {
    const conn = await pool.getConnection();
    await conn.ping();
    conn.release();
    return { success: true };
  } catch (error) {
    return { success: false, error: (error as Error).message };
  }
}

export default pool;
```

Always use parameterised queries — never string interpolation in SQL:

```typescript
// GOOD
await query('SELECT * FROM customers WHERE phone = ?', [phone]);

// BAD — SQL injection risk
await query(`SELECT * FROM customers WHERE phone = '${phone}'`);
```

---

## 9. MySQL Schema

### customers

```sql
CREATE TABLE customers (
  id            CHAR(36)      NOT NULL DEFAULT (UUID()),
  phone         VARCHAR(20)   NOT NULL UNIQUE,
  email         VARCHAR(255),
  name          VARCHAR(255),
  city          VARCHAR(100),
  preferred_model VARCHAR(50),
  lead_source   VARCHAR(50),
  created_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_phone (phone),
  INDEX idx_email (email)
);
```

### identity_map

```sql
-- Links anonymous browser IDs to known customer IDs
CREATE TABLE identity_map (
  anonymous_id  VARCHAR(128)  NOT NULL,
  customer_id   CHAR(36)      NOT NULL,
  resolved_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (anonymous_id),
  INDEX idx_customer (customer_id)
);
```

### scoring_rules

```sql
CREATE TABLE scoring_rules (
  id          INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  event_name  VARCHAR(100)  NOT NULL,
  conditions  JSON,           -- e.g. {"model": "RV400"}
  points      INT           NOT NULL,
  active      TINYINT(1)    NOT NULL DEFAULT 1,
  created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_event (event_name)
);
```

### customer_scores

```sql
CREATE TABLE customer_scores (
  customer_id   CHAR(36)     NOT NULL,
  score         INT          NOT NULL DEFAULT 0,
  intent_score  INT          NOT NULL DEFAULT 0,
  last_updated  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id)
);
```

### segments

```sql
CREATE TABLE segments (
  id          INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  name        VARCHAR(100)  NOT NULL,
  conditions  JSON          NOT NULL,  -- scoring + event conditions
  active      TINYINT(1)    NOT NULL DEFAULT 1,
  PRIMARY KEY (id)
);
```

### campaigns

```sql
CREATE TABLE campaigns (
  id          CHAR(36)      NOT NULL DEFAULT (UUID()),
  name        VARCHAR(255)  NOT NULL,
  status      ENUM('draft','active','paused','archived') NOT NULL DEFAULT 'draft',
  trigger_type ENUM('event','score','segment','schedule') NOT NULL,
  trigger_config JSON       NOT NULL,
  definition  JSON          NOT NULL,  -- React Flow nodes + edges
  version     INT UNSIGNED  NOT NULL DEFAULT 1,
  created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);
```

### campaign_executions

```sql
CREATE TABLE campaign_executions (
  id              CHAR(36)    NOT NULL DEFAULT (UUID()),
  campaign_id     CHAR(36)    NOT NULL,
  customer_id     CHAR(36)    NOT NULL,
  temporal_workflow_id VARCHAR(255) NOT NULL,
  status          ENUM('running','completed','cancelled','failed') NOT NULL DEFAULT 'running',
  started_at      DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ended_at        DATETIME,
  PRIMARY KEY (id),
  INDEX idx_campaign  (campaign_id),
  INDEX idx_customer  (customer_id),
  INDEX idx_workflow  (temporal_workflow_id)
);
```

### messages

```sql
CREATE TABLE messages (
  id            CHAR(36)    NOT NULL DEFAULT (UUID()),
  execution_id  CHAR(36)    NOT NULL,
  customer_id   CHAR(36)    NOT NULL,
  channel       ENUM('whatsapp','email','sms') NOT NULL,
  status        ENUM('pending','sent','delivered','read','failed') NOT NULL DEFAULT 'pending',
  provider_id   VARCHAR(255),   -- Meta message ID / SES message ID etc.
  payload       JSON        NOT NULL,
  sent_at       DATETIME,
  delivered_at  DATETIME,
  read_at       DATETIME,
  created_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_execution (execution_id),
  INDEX idx_customer  (customer_id)
);
```

---

## 10. ClickHouse Schema

### revolt_events (raw event store — written by LeadWorkers)

```sql
CREATE TABLE revolt_events (
  event_id      String,
  event_name    String,
  source        String,
  customer_id   String,
  anonymous_id  String,
  session_id    String,
  properties    String,       -- JSON string
  created_at    DateTime,
  updated_at    DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (customer_id, created_at);
```

---

## 11. Event Schema (Kafka payload standard)

All events published to Kafka must follow this shape:

```typescript
interface TrackingEvent {
  requestId:    string;        // UUID — dedup key
  event_name:   string;        // snake_case: "bike_viewed", "test_ride_submitted"
  source:       string;        // "website" | "whatsapp" | "calling" | "api"
  customer_id?: string;        // known customer ID (if resolved)
  anonymous_id: string;        // browser/device anonymous ID (always present)
  session_id?:  string;
  properties:   Record<string, unknown>;  // event-specific data
  ingestedAt:   string;        // ISO 8601
}
```

### Standard event names

| Event | Source | Key properties |
|-------|--------|----------------|
| `page_viewed` | website | `url`, `title`, `referrer` |
| `bike_viewed` | website | `model` |
| `price_viewed` | website | `model` |
| `emi_calculator_used` | website | `model`, `downPayment` |
| `dealer_selected` | website | `dealerId`, `city` |
| `test_ride_clicked` | website | `model` |
| `test_ride_submitted` | website | `model`, `dealerId` |
| `lead_form_submitted` | website | `model`, `phone`, `city` |
| `booking_started` | website | `model` |
| `booking_completed` | website | `model`, `bookingId` |
| `whatsapp_received` | whatsapp | `waId`, `messageType`, `message` |
| `whatsapp_replied` | whatsapp | `waId`, `campaignId` |

---

## 12. Worker Lifecycle Pattern

All Kafka consumer workers export `{ start, stop }` and are registered in `src/workers/index.ts`:

```typescript
// src/workers/index.ts
import eventsConsumer from './eventsConsumer.js';
// import scoringConsumer from './scoringConsumer.js';

const workers = [
  { name: 'events-consumer', module: eventsConsumer },
  // { name: 'scoring-consumer', module: scoringConsumer },
];

export default workers;
```

The entry point `src/index.ts` starts all workers and handles graceful shutdown — see `LeadWorkers/src/index.js` as the canonical implementation. Copy it as-is; change only the `service` label in the logger.

---

## 13. Temporal Patterns (campaign-engine only)

### Task Queue

```
revolt-campaign-queue
```

### Workflow naming

```
<campaignId>-<customerId>-<timestamp>
```

Example: `campaign_abc123-customer_xyz789-1723622400000`

### Campaign definition (stored in MySQL campaigns.definition)

```typescript
interface CampaignDefinition {
  nodes: CampaignNode[];
  edges: CampaignEdge[];
}

interface CampaignEdge {
  id:     string;
  source: string;  // node id
  target: string;  // node id
  label?: 'yes' | 'no';  // for condition nodes
}

type CampaignNode =
  | { id: string; type: 'trigger';   data: TriggerData }
  | { id: string; type: 'delay';     data: DelayData }
  | { id: string; type: 'whatsapp';  data: WhatsAppData }
  | { id: string; type: 'email';     data: EmailData }
  | { id: string; type: 'sms';       data: SmsData }
  | { id: string; type: 'condition'; data: ConditionData }
  | { id: string; type: 'end' };

interface DelayData     { duration: number; unit: 'minutes' | 'hours' | 'days' }
interface WhatsAppData  { templateId: string; variables: Record<string, string> }
interface EmailData     { subject: string; body: string; templateId?: string }
interface SmsData       { message: string }
interface ConditionData { field: string; operator: 'equals' | 'gte' | 'lte' | 'contains'; value: unknown }
```

### Generic Temporal Workflow (campaign-engine)

```typescript
// src/workflows/campaignWorkflow.ts
import { proxyActivities, sleep, CancelledFailure } from '@temporalio/workflow';
import type * as activities from '../activities/index.js';

const {
  getCampaignDefinition,
  sendWhatsApp,
  sendEmail,
  sendSms,
  evaluateCondition,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
});

export async function campaignWorkflow(campaignId: string, customerId: string): Promise<string> {
  const definition = await getCampaignDefinition(campaignId);
  const nodeMap = new Map(definition.nodes.map(n => [n.id, n]));
  const edgeMap = new Map<string, typeof definition.edges[0][]>();

  for (const edge of definition.edges) {
    if (!edgeMap.has(edge.source)) edgeMap.set(edge.source, []);
    edgeMap.get(edge.source)!.push(edge);
  }

  // Start at trigger node
  let currentId = definition.nodes.find(n => n.type === 'trigger')?.id;

  while (currentId) {
    const node = nodeMap.get(currentId);
    if (!node || node.type === 'end') break;

    let nextEdgeLabel: string | undefined;

    switch (node.type) {
      case 'trigger':
        break;

      case 'delay':
        await sleep(`${node.data.duration} ${node.data.unit}`);
        break;

      case 'whatsapp':
        await sendWhatsApp(customerId, node.data);
        break;

      case 'email':
        await sendEmail(customerId, node.data);
        break;

      case 'sms':
        await sendSms(customerId, node.data);
        break;

      case 'condition': {
        const result = await evaluateCondition(customerId, node.data);
        nextEdgeLabel = result ? 'yes' : 'no';
        break;
      }
    }

    // Follow next edge
    const edges = edgeMap.get(currentId) || [];
    const next = edges.find(e => !nextEdgeLabel || e.label === nextEdgeLabel) || edges[0];
    currentId = next?.target;
  }

  return 'completed';
}
```

### Campaign cancellation (exit conditions)

When a customer purchases or opts out, send a signal or cancel the workflow:

```typescript
// From any service:
const client = new Client({ connection });
await client.workflow.getHandle(workflowId).cancel();
```

---

## 14. Communication Service API

All outbound messages go through `communication-service`. Never call WhatsApp/email APIs directly from campaign-engine.

```
POST /send/whatsapp
POST /send/email
POST /send/sms
```

Request body:

```typescript
{
  customerId:  string;
  channel:     'whatsapp' | 'email' | 'sms';
  executionId: string;   // campaign_executions.id — for tracking
  payload: {
    // whatsapp
    to:         string;
    templateId: string;
    variables:  Record<string, string>;
    // email
    to:         string;
    subject:    string;
    body:       string;
    // sms
    to:         string;
    message:    string;
  };
}
```

Response:

```typescript
{
  success:    boolean;
  providerId: string;   // Meta message ID / SES ID etc.
  error?:     string;
}
```

---

## 15. Identity Resolution

When a user submits their phone number on the website:

1. SDK sends a `lead_form_submitted` event with `phone` + `anonymousId`
2. `customer-service` receives the event
3. Check if customer with that phone exists
   - Yes → link `anonymousId` → `customerId` in `identity_map`
   - No  → create customer, then link
4. Backfill ClickHouse: update `customer_id` on prior events where `anonymous_id` matches

This must happen before scoring and campaigns can act on those events.

---

## 16. Scoring Rules Evaluation

```
Event arrives
     │
     ▼
Match event_name against scoring_rules
     │
     ▼
For each matching rule, evaluate conditions (JSON)
     │
     ▼
Sum points
     │
     ▼
UPDATE customer_scores SET score = score + ? WHERE customer_id = ?
     │
     ▼
Check segment membership
     │
     ▼
Publish scoring.updates.v1
     │
     ▼
campaign-engine evaluates if a campaign should start
```

---

## 17. Campaign Trigger Evaluation

```typescript
// scoring.updates.v1 consumer in campaign-engine
async function onScoreUpdate(event: ScoreUpdateEvent) {
  const activeCampaigns = await getActiveCampaigns({ triggerType: 'score' });

  for (const campaign of activeCampaigns) {
    const matches = evaluateTrigger(campaign.trigger_config, event);
    if (!matches) continue;

    const alreadyRunning = await getActiveExecution(campaign.id, event.customerId);
    if (alreadyRunning) continue;  // don't start duplicate

    await temporalClient.workflow.start('campaignWorkflow', {
      taskQueue: config.temporal.taskQueue,
      workflowId: `${campaign.id}-${event.customerId}-${Date.now()}`,
      args: [campaign.id, event.customerId],
    });

    await recordExecution(campaign.id, event.customerId, workflowId);
  }
}
```

---

## 18. Graceful Shutdown (all services)

Every service must handle `SIGINT` and `SIGTERM`:

```typescript
const SHUTDOWN_TIMEOUT_MS = 15000;
let isShuttingDown = false;

async function shutdown(signal: string) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info(`${signal} received, shutting down...`);

  const timer = setTimeout(() => { logger.error('Shutdown timeout'); process.exit(1); }, SHUTDOWN_TIMEOUT_MS);
  timer.unref();

  // stop workers / close connections
  await stopAll();
  process.exit(0);
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException',  err => { logger.error('uncaughtException', { error: err.message }); shutdown('uncaughtException'); });
process.on('unhandledRejection', err => { logger.error('unhandledRejection', { error: String(err) }); shutdown('unhandledRejection'); });
```

---

## 19. Error Handling Rules

1. Never swallow errors silently — always log with `error` level + stack
2. Kafka consumer errors: log + continue (don't crash the worker)
3. ClickHouse insert failure: re-buffer the batch + retry on next flush
4. MySQL errors: surface to caller, let the route/worker decide retry vs fail
5. Temporal activity failures: let Temporal retry per `maximumAttempts` config
6. HTTP 5xx: log + return `{ success: false, error: "message" }` — never expose stack to client

---

## 20. Docker Convention

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY src ./src
COPY .env.example .env.example
CMD ["node", "src/index.js"]
```

Environment variables are injected at runtime (ECS task definition / docker-compose). Never bake `.env` into the image.

---

## 21. Versioning and Campaign Immutability

When a user publishes a campaign:

1. Save a new row in `campaigns` with `version = version + 1`
2. Existing running `campaign_executions` continue on their version
3. New leads use the latest `active` version
4. Never mutate a campaign definition that has running executions

---

## 22. Quick Reference — Build Order

```
Phase 1  →  LeadWorkers (done) + event-service + revolt-analytics-sdk
Phase 2  →  customer-service (profiles + identity resolution)
Phase 3  →  scoring-service (rules + scores + segments)
Phase 4  →  campaign-builder (React Flow UI + campaign CRUD API)
Phase 5  →  campaign-engine (Temporal workers + generic workflow)
Phase 6  →  communication-service (WhatsApp + Email + SMS)
Phase 7  →  Analytics dashboards (ClickHouse queries on revolt_events)
```
