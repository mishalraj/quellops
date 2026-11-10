# DB Observability Generic Work Package

**Purpose:** carve out a standalone engineering task for database signal ingestion that is safe for production and not tied to a Postgres-only continuous scraper.

**Owner:** Unassigned
**Status:** Proposed
**Priority:** High (POC-to-prod hardening prerequisite)

---

## 1) Why this is needed

Current implementation is useful for local demos but not production-safe as-is:

- scraper is currently Postgres-specific (`pg_stat_statements`),
- scraper runs continuously on a fixed schedule,
- query polling can add avoidable read load on production DBs,
- host URL wiring is environment-specific in manifest defaults.

We need a **generic, low-overhead DB signal framework** that still detects:

1. DB behavior changes (regressions/drift),
2. DB latency/error spikes,
3. query-level hotspots (when available),
4. correlation with service anomalies in the incident engine.

---

## 2) Scope

### In scope

- Design and implement a database-agnostic signal ingestion contract.
- Support at least one non-Postgres source path (or generic abstraction + Postgres adapter).
- Replace always-on fixed scraping with controlled collection strategies (adaptive/sampled/event-driven where possible).
- Add production safety controls (rate limits, timeouts, backoff, kill switch).
- Emit normalized DB change/spike events into `POST /ingest/change`.
- Extend docs and runbooks with safe defaults and rollout guidance.

### Out of scope (for this package)

- Long-term historical data warehouse design.
- Full multi-tenant auth/RBAC layer.
- Perfect SQL normalization across every database vendor.

---

## 3) Functional requirements

| ID | Requirement | Must have |
|---|---|---|
| FR-1 | Generic DB signal schema | Yes |
| FR-2 | Adapter model (`postgres`, `mysql`, `mongo` or placeholder generic adapter) | Yes |
| FR-3 | Emit normalized events for spike/change detection | Yes |
| FR-4 | Query-level evidence when available, metric-level fallback when not | Yes |
| FR-5 | Configurable collection interval and sampling | Yes |
| FR-6 | Enable/disable per environment via config flags | Yes |

---

## 4) Non-functional and safety requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-1 | Minimal DB overhead | Collection should stay low-impact under normal load |
| NFR-2 | Hard timeouts | Every DB probe must time out quickly |
| NFR-3 | Backoff on failure | Exponential backoff + jitter on repeated errors |
| NFR-4 | Circuit breaker / kill switch | Immediate disable without redeploy |
| NFR-5 | Bounded payloads | Limit rows/statements per cycle |
| NFR-6 | Observability | Export scraper self-metrics (duration, failures, dropped samples) |

---

## 5) Proposed architecture (for engineer to evaluate)

### 5.1 Normalized DB signal contract

Add/standardize change events with a stable `kind` family, for example:

- `db.latency_spike`
- `db.error_spike`
- `db.query_hotspot`
- `db.schema_change` (future input)

Suggested metadata shape:

- `db_engine`, `instance`, `database`, `service_entity`
- `metric_name`, `current_value`, `baseline_value`, `delta`
- `window_s`, `sample_count`, `source_adapter`
- optional `query_fingerprint` and redacted query text

### 5.2 Adapter abstraction

Define adapter interface:

1. collect raw stats safely,
2. normalize to internal DB signal objects,
3. apply per-adapter limits/redaction,
4. emit to backend change ingest endpoint.

Start with:

- Adapter A: Postgres (`pg_stat_statements`) hardened path,
- Adapter B: generic metric adapter (from DB exporter / telemetry source) if query text is unavailable.

### 5.3 Collection strategy options

Engineer should choose and document one primary strategy:

- fixed low-frequency polling + adaptive slowdown,
- threshold-triggered polling (faster only during anomalies),
- pull from existing observability pipeline (preferred when available),
- hybrid approach.

---

## 6) Engine integration requirements

Current engine already consumes `db.slow_query_stat` and `db_slow_query` candidates.
This package should:

- keep backward compatibility for existing demo events,
- add generic DB event mapping to incident candidate generation,
- include anti-noise scoring guards (minimum samples, recency decay, entity matching).

---

## 7) Deliverables

| Deliverable | Description |
|---|---|
| D1 | Technical design doc (chosen architecture + trade-offs) |
| D2 | Adapter interface + at least one hardened adapter implementation |
| D3 | Config model (enable flags, intervals, limits, timeout, backoff) |
| D4 | Backend mapping updates for new DB signal kinds |
| D5 | Safety test report (load impact and failure behavior) |
| D6 | Runbook updates for production-safe rollout |

---

## 8) Acceptance criteria

| ID | Acceptance criteria |
|---|---|
| AC-1 | DB scraping/collection can be disabled instantly via config |
| AC-2 | Collection path is not hardcoded to one host style or one DB vendor |
| AC-3 | Under normal settings, no measurable harmful DB load increase in validation environment |
| AC-4 | DB spike/change events are visible in backend and influence incident evidence correctly |
| AC-5 | Failure modes degrade gracefully (timeouts/backoff, no crash loops) |
| AC-6 | Documentation includes safe defaults for production rollout |

---

## 9) Suggested implementation phases

| Phase | Goal | Output |
|---|---|---|
| P1 | Design + interface | Design doc + adapter contract |
| P2 | Harden existing Postgres path | Safe Postgres adapter + config controls |
| P3 | Generic path | Additional adapter or generic metrics ingestion path |
| P4 | Engine and docs | Updated mappings, tests, rollout runbook |

---

## 10) Handoff checklist for assigned engineer

- Confirm target production environments and allowed DB access patterns.
- Decide primary collection strategy and fallback mode.
- Define minimal event schema and redaction policy.
- Implement safety controls first (timeouts, backoff, kill switch).
- Validate impact in staging before any production rollout.
- Demo: show one real DB spike/change reflected in incident evidence.
