# LeadWorkers — Architecture & Flow

## Overview

LeadWorkers is a standalone Node.js Kafka consumer that processes raw website/app events and builds a lead scoring, segmentation, and lifecycle tracking system for Revolt Motors.

```
┌──────────────┐     ┌───────────────────────────────────┐     ┌──────────────┐
│    Kafka     │────▶│          LeadWorkers               │────▶│  ClickHouse  │
│  (raw events)│     │  (score, segment, lifecycle)       │     │  (analytics) │
└──────────────┘     └──────────┬────────────┬────────────┘     └──────────────┘
                                │            │
                                ▼            ▼
                         ┌──────────┐  ┌──────────────┐
                         │  MySQL   │  │ revolt-engage│
                         │(14 tables)│  │  (journeys)  │
                         └──────────┘  └──────────────┘
                                │
                                ▼
                         ┌──────────┐
                         │  Redis   │
                         │ (cache)  │
                         └──────────┘
```

---

## Project Structure

```
src/
├── index.js                  ← Entry point: starts all workers
├── config/index.js           ← Centralized env config
├── lib/
│   ├── db.js                 ← MySQL connection pool
│   ├── redis.js              ← Redis cache (hot-path lookups)
│   ├── kafka.js              ← Kafka client (SSL, consumer factory)
│   ├── clickhouse.js         ← ClickHouse client (insert + query)
│   ├── logger.js             ← Winston structured logging
│   └── engageSignal.js       ← Fire-and-forget HTTP to revolt-engage
├── handlers/
│   ├── registry.js           ← Maps source name → handler module
│   └── website/index.js      ← Website-specific payload transform
├── services/
│   ├── IdentityService.js    ← Who is this visitor?
│   ├── ScoringService.js     ← How many points?
│   ├── SegmentService.js     ← WARM / HOT / HOTTEST?
│   └── LifecycleService.js   ← Which funnel stage?
└── workers/
    ├── index.js              ← Worker registry
    ├── eventsConsumer.js     ← Kafka consumer (main pipeline)
    └── decayWorker.js        ← Scheduled decay (every 12h)
```

---

## Workers

### 1. eventsConsumer (Kafka consumer — long-running)

Reads events from Kafka topic `webevents.v1.json` and processes each message through the full pipeline.

### 2. decayWorker (Scheduled — every 12 hours)

Finds customers inactive for 21+ days and applies score decay. Processes in batches of 200. Never demotes segment.

---

## Event Processing Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        KAFKA MESSAGE ARRIVES                         │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │   1. Route to Source Handler │
                    │   (registry.js → website)   │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │   2. Transform Payload       │
                    │   → ClickHouse row shape     │
                    └──────────────┬──────────────┘
                                   │
                          ┌────────┴────────┐
                          │                 │
                ┌─────────▼──────┐  ┌───────▼────────┐
                │ IDENTITY EVENT │  │ PASSIVE EVENT  │
                │ (form submit)  │  │ (page view etc)│
                └────────┬───────┘  └───────┬────────┘
                         │                  │
                         ▼                  ▼
          ┌──────────────────────┐  ┌──────────────────┐
          │ IdentityService      │  │ resolveCustomerId│
          │ .identifyVisitor()   │  │ (Redis → MySQL)  │
          │                      │  │                  │
          │ • find customer by   │  │ Returns:         │
          │   phone/email        │  │ • customerId     │
          │ • create if new      │  │ • or NULL        │
          │ • link anonymous_id  │  │   (anonymous)    │
          │ • trigger backfill   │  │                  │
          └──────────┬───────────┘  └────────┬─────────┘
                     │                       │
                     │            ┌──────────┴──────────┐
                     │            │  NULL = anonymous   │
                     │            │  → SKIP scoring     │
                     │            │  → just write to    │
                     │            │    ClickHouse       │
                     │            └─────────────────────┘
                     │
                     ▼
          ┌──────────────────────────────────────────┐
          │         3. ScoringService.scoreEvent()    │
          │                                          │
          │  a) Check last_seen_at                   │
          │     → 21-45 days inactive? drop 30%      │
          │     → 46+ days? reset to 0               │
          │                                          │
          │  b) Look up event points                 │
          │     (source_event_scores table via Redis) │
          │                                          │
          │  c) Add points to customer_scores        │
          │     Write audit row to score_events      │
          │     Update last_seen_at                  │
          └────────────────────┬─────────────────────┘
                               │
                               ▼
          ┌──────────────────────────────────────────┐
          │      4. SegmentService.recompute()        │
          │                                          │
          │  a) journey_stop event?                  │
          │     → demote one level (HOTTEST→HOT)     │
          │                                          │
          │  b) Pause event? (test_ride_booking)     │
          │     → set is_paused = 1                  │
          │                                          │
          │  c) Instant-flag event?                  │
          │     (booking_page_viewed, emi_calc, etc) │
          │     → force HOTTEST immediately          │
          │                                          │
          │  d) Score-based promotion                │
          │     1-49 → WARM                          │
          │     50-99 → HOT                          │
          │     100+ → HOTTEST                       │
          │     (only upward, never down)            │
          │                                          │
          │  ✦ On segment change:                    │
          │    → dispatch signal to revolt-engage    │
          └────────────────────┬─────────────────────┘
                               │
                               ▼
          ┌──────────────────────────────────────────┐
          │     5. LifecycleService.advance()         │
          │                                          │
          │  Look up lifecycle_stage_triggers:        │
          │    contact_form    → LEAD_FILLED         │
          │    test_ride_booking → TEST_RIDE_BOOKED  │
          │    no_show_marked  → NO_SHOW             │
          │    book_bike       → BOOKING_CREATED     │
          │    retail_completed → RETAIL_COMPLETED   │
          │                                          │
          │  Rules:                                  │
          │    • Forward-only (no regression)        │
          │    • Exception: NO_SHOW allows re-entry  │
          │                                          │
          │  ✦ On stage change:                      │
          │    → dispatch signal to revolt-engage    │
          └────────────────────┬─────────────────────┘
                               │
                               ▼
          ┌──────────────────────────────────────────┐
          │  6. Dispatch behavioural signal           │
          │     to revolt-engage (fire-and-forget)   │
          │                                          │
          │  Every event for a known customer:       │
          │  emi_calculator_used, return_visit, etc. │
          │  → POST /api/signals to journey engine   │
          └────────────────────┬─────────────────────┘
                               │
                               ▼
          ┌──────────────────────────────────────────┐
          │  7. Buffer row → flush to ClickHouse     │
          │     (batch: 50 rows or 5s timeout)       │
          └──────────────────────────────────────────┘
```

---

## Decay Worker Flow

```
┌──────────────────────────────────────────────────────┐
│              EVERY 12 HOURS                           │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ Load active sources    │
              │ (score_decay_rules)    │
              └───────────┬────────────┘
                          │
                          ▼  for each source
              ┌────────────────────────┐
              │ Find customers where:  │
              │ • score > 0            │
              │ • last_seen_at < 21d   │
              │ • not decayed in 12h   │
              └───────────┬────────────┘
                          │
                          ▼  for each customer (batch 200)
              ┌────────────────────────┐
              │ Apply decay rule:      │
              │ 21-45d → score × 0.70  │
              │ 46d+   → score = 0     │
              │                        │
              │ Write __decay__ to     │
              │ score_events (audit)   │
              │                        │
              │ Recompute segment      │
              │ (NEVER demotes)        │
              └────────────────────────┘
```

---

## Services & Tables

### IdentityService

**Purpose:** Resolve who an anonymous visitor is, create/link customers.

```
                   ┌─────────────────────────┐
                   │    IdentityService       │
                   └────────────┬────────────┘
                                │
              ┌─────────────────┼──────────────────┐
              │                 │                   │
              ▼                 ▼                   ▼
   ┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
   │anonymous_profiles│ │  customers   │ │ identity_merges  │
   │                  │ │              │ │                  │
   │• anonymous_id    │ │• id          │ │• customer_id     │
   │• customer_id     │ │• phone       │ │• anonymous_id    │
   │• source          │ │• email       │ │• source          │
   │• identified_at   │ │• name        │ │• reason          │
   └──────────────────┘ └──────────────┘ └──────────────────┘
```

**Redis cache:** `anon:{source}:{anonymous_id}` → customerId (24h TTL)

---

### ScoringService

**Purpose:** Add/remove points, handle decay, track every change.

```
                   ┌─────────────────────────┐
                   │    ScoringService        │
                   └────────────┬────────────┘
                                │
         ┌──────────────────────┼───────────────────────┐
         │                      │                       │
         ▼                      ▼                       ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│ customer_scores  │  │  score_events    │  │ source_event_scores  │
│                  │  │                  │  │       (config)       │
│• customer_id     │  │• customer_id     │  │                      │
│• score           │  │• event_name      │  │• source              │
│• last_seen_at    │  │• score_delta     │  │• event_name          │
│                  │  │• is_backfill     │  │• score (points)      │
└──────────────────┘  └──────────────────┘  └──────────────────────┘
                                                       │
                                            ┌──────────┴───────────┐
                                            │ score_decay_rules    │
                                            │       (config)       │
                                            │                      │
                                            │• days_from / days_to │
                                            │• decay_type          │
                                            │• decay_value         │
                                            └──────────────────────┘
```

**Redis cache:** `score_config:{source}` → { event_name: points } (5min TTL)

---

### SegmentService

**Purpose:** Assign WARM/HOT/HOTTEST bucket, handle instant flags, pause, demotion.

```
                   ┌─────────────────────────┐
                   │    SegmentService        │
                   └────────────┬────────────┘
                                │
         ┌──────────────────────┼───────────────────────┐
         │                      │                       │
         ▼                      ▼                       ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│ customer_scores  │  │ segment_history  │  │   segment_rules      │
│                  │  │                  │  │       (config)       │
│• segment (R/W)   │  │• from_segment    │  │                      │
│• is_paused (R/W) │  │• to_segment      │  │• min_score           │
│• paused_at (R/W) │  │• changed_by      │  │• max_score           │
│                  │  │• event_name      │  │• daily_touch_cap     │
└──────────────────┘  └──────────────────┘  └──────────────────────┘
                                                       │
                                            ┌──────────┴───────────┐
                                            │ instant_flag_events  │
                                            │       (config)       │
                                            │                      │
                                            │• event_name          │
                                            │• target_segment      │
                                            └──────────────────────┘
```

**Rules:**
- Promotion: only upward (WARM → HOT → HOTTEST)
- Demotion: only on `journey_stop` event
- Instant flag: booking_page_viewed / emi_calculator / etc → force HOTTEST
- Pause: test_ride_booking / book_bike → is_paused = 1

---

### LifecycleService

**Purpose:** Track which stage of the purchase funnel each customer is in.

```
                   ┌─────────────────────────┐
                   │   LifecycleService       │
                   └────────────┬────────────┘
                                │
         ┌──────────────────────┼───────────────────────┐
         │                      │                       │
         ▼                      ▼                       ▼
┌──────────────────────┐ ┌──────────────────┐ ┌────────────────────────┐
│ customer_lifecycle   │ │lifecycle_history │ │lifecycle_stage_triggers│
│                      │ │                  │ │        (config)        │
│• current_stage       │ │• stage           │ │                        │
│• previous_stage      │ │• entered_at      │ │• event_name            │
│• entered_at          │ │• exited_at       │ │• to_stage              │
└──────────────────────┘ └──────────────────┘ │• is_regression_allowed │
                                              └────────────────────────┘
```

**Funnel stages (ordered):**
```
LEAD_FILLED → LEAD_SCORED → TEST_RIDE_BOOKED → TEST_RIDE_SCHEDULED
  → TEST_RIDE_COMPLETED | NO_SHOW → BOOKING_STARTED → BOOKING_CREATED
  → RETAIL_COMPLETED
```

---

## Tables Summary (14 total)

| # | Table | Type | Written by |
|---|---|---|---|
| 1 | `customers` | Data | IdentityService |
| 2 | `anonymous_profiles` | Data | IdentityService |
| 3 | `identity_merges` | Audit | IdentityService |
| 4 | `customer_scores` | Data | ScoringService + SegmentService |
| 5 | `score_events` | Audit | ScoringService |
| 6 | `source_event_scores` | Config | Seed / manual |
| 7 | `score_decay_rules` | Config | Seed / manual |
| 8 | `segment_rules` | Config | Seed / manual |
| 9 | `instant_flag_events` | Config | Seed / manual |
| 10 | `segment_history` | Audit | SegmentService |
| 11 | `lifecycle_stage_triggers` | Config | Seed / manual |
| 12 | `customer_lifecycle` | Data | LifecycleService |
| 13 | `lifecycle_history` | Audit | LifecycleService |
| 14 | (none — `customer_scores` has segment/pause columns) | — | — |

---

## External Systems

| System | Role |
|---|---|
| **Kafka** | Input: raw events from website |
| **ClickHouse** | Output: every raw event stored for analytics |
| **MySQL** | All business state (14 tables) |
| **Redis** | Cache: identity lookups, score config, segment rules |
| **revolt-engage** | Receives signals to start/control customer journeys |

---

## Signal Flow to revolt-engage

```
LeadWorkers                              revolt-engage
───────────                              ─────────────

SegmentService                           Journey Lifecycle
  segment changes ────────────────────▶  segment_warm → starts WARM workflow
  (segment_warm/hot/hottest)             segment_hot  → cancels WARM, starts HOT
                                         segment_hottest → cancels HOT, starts HOTTEST

LifecycleService                         Journey Lifecycle
  stage transitions ──────────────────▶  test_ride_booked → cancels lead journey,
  (test_ride_booked, no_show, etc.)                         starts Pre-Ride workflow
                                         no_show_marked → starts No-Show workflow
                                         test_ride_completed → starts Post-Ride workflow
                                         booking_created → starts Booking-Retail workflow

eventsConsumer                           "Whichever comes first" signals
  behavioural events ─────────────────▶  emi_calculator_used → wakes sleeping workflow
  (emi_calc, return_visit, etc.)         test_ride_page_viewed → fires step early
                                         return_visit → fires step early
```

All signals are fire-and-forget. If revolt-engage is down, LeadWorkers continues normally.
