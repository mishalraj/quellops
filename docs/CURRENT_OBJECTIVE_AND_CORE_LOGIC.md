# Current Objective And Core Logic

This document explains what the project is trying to achieve right now, and how the main logic works in the current codebase.

Last updated: 2026-06-01

---

## 1) Current objective (POC phase)

The current objective is to make `aidbg` reliable and demo-ready on a local machine:

1. ingest traces and Kubernetes/change signals in near real time,
2. detect service anomalies from trace windows,
3. produce deterministic top-cause hypotheses (not opaque ML),
4. present evidence in the UI clearly enough for fast debugging demos,
5. validate behavior through scripted incident scenarios on a small, controlled service mesh.

This is a **POC-polish-first** goal. MVP-hardening work (durability, jobs, advanced scoring) is intentionally deferred.

---

## 2) Current system shape

### 2.1 Runtime components

| Component | Responsibility | Path |
|------|------|------|
| Backend (FastAPI, Python) | OTLP ingest, signal store, anomaly detection, incident assembly, explain endpoint, UI serving | `backend/src/aidbg/` |
| Agent (Go) | Watches K8s objects/events and sends normalized events to backend | `agent/` |
| Collector (K8s) | Receives OTLP in-cluster and exports to backend OTLP gRPC endpoint | `deploy/otel-collector/collector.yaml` |
| Demo mesh (4 services) | Deterministic topology for testing and demos | `demo/services/` + `deploy/aidbg-demo/` |
| Scenario tooling | Fault injection, load, assertions, artifacts | `scripts/run_demo_scenario.sh`, `scripts/run_all_demo_scenarios.sh`, `scenarios/demo/` |

### 2.2 Data paths

1. Services emit traces -> collector -> backend OTLP ingest.
2. K8s agent posts deployment/pod/event signals -> `POST /ingest/k8s`.
3. Optional normalized changes go to `POST /ingest/change` (and GitHub webhook maps pushes to changes).
4. Backend keeps an in-memory rolling signal store and serves snapshots and hypotheses.

---

## 3) Engine logic in depth

This section describes exactly how `backend/src/aidbg/engine.py` turns raw signals into an incident.

### 3.1 SignalStore data model and lifecycle

`SignalStore` maintains in-memory rolling deques:

| Store | Type | Source | Used by |
|---|---|---|---|
| `_spans` | `deque[SpanPoint]` | OTLP ingest | anomalies, service metrics, DB candidates, trace focus |
| `_edges` | `deque[(at_s, src, dst, latency_ms, is_error)]` | OTLP parent/child cross-service links | graph snapshot, entity correlation |
| `_k8s_events` | `deque[K8sSignal]` | `POST /ingest/k8s` from agent | deploy/dependency candidates |
| `_change_events` | `deque[ChangeEvent]` | `POST /ingest/change`, GitHub webhook mapper, pg-stat scraper | code/config/db change candidates |

Lifecycle behavior:

1. ingest appends to deques,
2. `_trim()` drops entries older than horizon (default bounded memory behavior),
3. query endpoints (`/incident/latest`, `/anomalies/latest`, `/query/*`) compute from current snapshots.

### 3.2 OTLP-to-analysis normalization

In `ingest_otlp.py`:

- each span is normalized to `SpanPoint` with:
  - timing (`start_unix_s`, `at_unix_s`, `latency_ms`),
  - identity (`trace_id`, `span_id`, `parent_span_id`, `service`),
  - error bit (`status.code == 2`),
  - DB markers (`db.system`, `db.statement`, inferred SQL name prefixes).
- cross-service edges are created only when parent and child map to different services.

This creates a service-level graph from distributed traces without needing external storage.

### 3.3 Windowing strategy

Configured in `config.py`:

- `current_window_s` (default `300s`)
- `baseline_window_s` (default `900s`)

Computation windows:

- current: `[now - current_window_s, now]`
- baseline: `[now - current_window_s - baseline_window_s, now - current_window_s]`

This gives short-term behavior vs recent historical behavior from the same process lifetime.

### 3.4 Anomaly detector internals (`detect_anomalies`)

Per service:

1. gather current and baseline points,
2. require minimum sample size in both windows (`>= 20` and `>= 20`),
3. compute:
   - `cur_p95`, `base_p95` (with floor `base_p95 >= 1.0`),
   - current and baseline error rates,
4. emit anomalies if:
   - latency ratio `cur_p95 / base_p95 >= 1.5` -> `latency_spike`
   - error delta `cur_err - base_err >= 0.05` -> `error_spike`

Output fields include entity, magnitude, and baseline/current evidence used later by incident ranking and UI.

### 3.5 Candidate generation in `build_incident()`

`build_incident()` produces candidate causes in parallel families, then merges and ranks.

#### A) `recent_deploy` candidates

Source: recent K8s events (`limit=200`) that look like deployment/rollout.

Filters and scoring:

- ignore if older than `1800s`,
- ignore scale-to-zero deploy events (`desired_replicas == "0"`) to avoid misclassifying outage as rollout,
- base score: `max(0.1, 1.0 - age_s/1800)`,
- `+0.08` if rollout is in-progress (`deploy.started` style),
- `+0.25` if event entity matches anomaly entity,
- `+0.12` if service has high trace traffic (`request_count >= 80`),
- capped at `1.2`.

Tie-breaker:

- starts with `-age_s` (newer wins),
- boost when anomalous service has lower traffic than peers (`lower_traffic_anomaly`) to surface likely leaf fault targets (important for deploy-latency scenarios).

Dedup key: `(entity, namespace)` keeps highest-score row.

#### B) `dependency_unavailable` candidates

Two paths:

1. scale-to-zero path (`desired_replicas == "0"`):
   - score band around `1.32` decaying with age,
   - intentionally above `recent_deploy` cap so outage beats rollout noise.
2. pod/event failure hints:
   - requires `pod`/`event` type and message/reason hints (`back-off`, `crashloop`, `unhealthy`),
   - lower score band (`~0.9` max) decayed by age.

Dedup key: `(entity, namespace)`.

#### C) `traffic_spike` candidates

From `service_metrics(current_window)`:

- requires `request_count >= 380`,
- requires low error (`error_rate < 0.01`),
- score grows with excess requests, capped at `1.26`.

This lets sustained clean traffic outrank stale deploy noise in load scenarios.

#### D) DB and change-correlation candidates

`db_slow_query_candidates()`:

- DB span groups by `(service, normalized_statement)`,
- requires at least `5` DB spans,
- candidate emitted if:
  - `cur_p95 >= db_slow_threshold_ms` (default `100ms`), or
  - latency ratio vs baseline `>= 1.5`,
- score includes affected-entity bonus.

`change_event_candidates()`:

- consumes normalized change events within `change_horizon_s` (default `900s`),
- base weights by kind (`code.push`, `config.changed`, `db.migration.applied`, `db.slow_query_stat`),
- applies time decay and entity-match bonuses,
- dedups by `(type, entity)`.

#### E) Merge and final top causes

1. merge all candidate families,
2. sort by `(score, tie_breaker)` descending,
3. fallback to `unknown_regression` only when anomalies exist and no candidate exists,
4. `_pick_top_causes()` returns top-3 with diversity rule: max two `traffic_spike` rows, then backfill by score.

### 3.6 Query-oriented evidence endpoints

- `why_service_slow(service)` = service-specific anomalies + global top causes.
- `evidence_bundle(service)` = service metrics + adjacent graph edges + anomalies + DB candidates + related changes.

These endpoints are designed for explainability/debug workflows, not just dashboard tiles.

### 3.7 Trace-focus path selection

`trace_focus_snapshot(service, window_s)`:

1. select traces touching target service,
2. rank traces preferring:
   - target-service errors,
   - higher target-service latency,
   - recency,
3. keep target subtree + ancestors,
4. return timeline rows with depth/start/duration/error flags.

This is the backend for the focused trace/Gantt UI panel.

### 3.8 Explain layer contract

`/explain/latest` always starts from deterministic incident JSON.

- with Azure OpenAI config: narrative explanation is generated,
- without config: deterministic fallback payload.

So explanatory text can vary, but core cause ranking does not depend on LLM output.

---

## 4) How scenarios validate the logic

Each file in `scenarios/demo/` defines:

- target deployment,
- injection type (latency/error/scale/load),
- waits/load parameters,
- assertions (expected cause type/entity/top-k behavior/min spans).

`scripts/run_demo_scenario.sh`:

1. applies injection,
2. drives load,
3. waits for propagation,
4. captures `incident/anomalies/graph/explain` artifacts,
5. validates assertions.

This gives deterministic pass/fail checks for ranking behavior changes.

---

## 5) How false alerts are reduced right now

The system is heuristic, but it contains several explicit anti-noise controls.

| Noise risk | Current mitigation in engine |
|---|---|
| Low-volume random spikes | anomaly detector requires minimum samples in both windows (`>=20`) |
| Division artifacts on tiny baselines | baseline p95 floored to `>=1.0ms` |
| Old stale events dominating ranking | time-decay scores + horizon cutoffs (`1800s` for deploy/dependency, `900s` for change events default) |
| Scale-down mistaken as deploy issue | deploy candidates explicitly skip `desired_replicas == 0`; treated as `dependency_unavailable` |
| Deployment event flood duplicates | dedup by `(entity, namespace)` for deploy/dependency; highest score kept |
| Traffic spikes during error storms | `traffic_spike` requires very low error rate (`<1%`) |
| Traffic candidates crowding all top slots | top-cause selector caps `traffic_spike` to max two rows |
| DB query blips | DB candidates require minimum query count (`>=5`) and threshold/ratio gates |
| Unrelated changes over-attributed | change candidates are time-decayed and boosted only when entity-graph/anomaly match exists |
| Tie instability between similar scores | tie-breakers include recency and lower-traffic-anomaly hint for deploy signals |

Important caveat:

- These mitigations reduce false alerts, but do not eliminate them.
- Because all data is in-memory and short-windowed, startup periods or abrupt topology churn can still cause transitional mis-ranking.

---

## 6) Current boundaries and known constraints

- In-memory store only (restart clears history).
- Rankings are heuristic and deterministic; this is intentional for repeatability.
- Scenario realism is bounded by the synthetic demo mesh.
- Local host-to-cluster addressing still needs environment awareness (`host.docker.internal` vs bridge IP).

---

## 7) What “good” looks like before MVP-hardening

For POC sign-off, we should have:

1. one-command local startup working consistently,
2. all demo scenarios green on the latest ranking logic,
3. incident and UI outputs stable and understandable,
4. docs aligned to one canonical operator workflow.

Once this is stable, next-phase MVP work can move to persistence, background jobs, and richer correlation scoring.
