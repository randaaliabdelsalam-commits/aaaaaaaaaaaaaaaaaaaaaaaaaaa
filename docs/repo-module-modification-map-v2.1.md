# Repository Module-to-Modification Map (Zoho v2.1)

## 1) `sync/token_manager.py`

### Planned concrete modifications
- Add region-aware auth endpoint selection (e.g., `accounts.zoho.com`, `.in`, `.eu`).
- Store absolute expiry timestamp (`expires_at_epoch`) instead of relative `expires_in` only.
- Harden refresh failure handling:
  - `invalid_grant` / revoked refresh token => non-retryable auth incident.
  - transient network/timeouts => retryable with bounded retries at token layer.
- Emit structured auth metrics:
  - refresh_success_count
  - refresh_failure_count
  - token_age_seconds

## 2) `sync/zoho_client.py`

### Planned concrete modifications
- Introduce normalized response classifier used by all call sites.
- Keep existing retry behavior but add:
  - one-refresh-per-attempt guard for repeated 401 loops,
  - explicit 403/404/409 handling paths,
  - circuit-breaker hooks for sustained 5xx.
- Add endpoint-type abstraction for Creator vs CRM v8 pathing.
- Add optional idempotency key headers (when endpoint supports custom headers).
- Expand telemetry:
  - latency histograms by method/module,
  - retry_count, retry_reason, throttle events.

## 3) `sync/realtime_worker.py`

### Planned concrete modifications
- Preserve `FOR UPDATE SKIP LOCKED` claim behavior.
- Add deterministic dedupe window for duplicate events.
- Add stricter event classification:
  - retryable outbound errors => reschedule with computed delay,
  - mapping/payload errors => mark DEAD with typed reason codes.
- Persist reconciliation hints in `last_error` JSON format (short code + detail).

## 4) `sync/backfill_worker.py`

### Planned concrete modifications
- Partition backfill scans by key ranges/time windows.
- Enforce lower limiter priority than realtime.
- Write progress checkpoints (high-water marks) to resumable state table.
- Support bulk APIs for large volumes when configured.

## 5) `sync/dead_worker.py`

### Planned concrete modifications
- Add dead-letter replay policy by error class:
  - auth/config fixed => replay eligible,
  - schema mismatch => hold until mapping fix,
  - permanent data invalid => terminal dead.
- Add max replay budget per cycle to avoid starving realtime.
- Attach operator annotations for manual resolution outcomes.

## 6) `sync/connectivity.py`

### Planned concrete modifications
- Standardize internet-health probes with per-target checks.
- Add jittered backoff utility shared by all workers.
- Emit connectivity state transitions for alerting (up/down/flapping).

## 7) SQL migration files (`sql/*.sql`)

### Planned concrete modifications
- Add columns to `SYNC_EVENTS` (or sibling tables) for standardized operations:
  - `error_code`, `error_class`, `retry_after_seconds`, `replayable_flag`,
  - `dedupe_key`, `trace_id`, `last_attempt_at`.
- Add index support:
  - `(status, next_attempt_at)` composite,
  - `(source_table, k_cp, k_yr, k_code, k_bn)` for reconciliation joins,
  - optional unique/partial index for dedupe windows.
- Add reconciliation run tables:
  - `SYNC_RECON_RUN`, `SYNC_RECON_DIFF`.

## 8) Tests

### Files in scope
- `tests/test_token_manager.py`
- `tests/test_zoho_client.py`
- `tests/test_realtime_worker.py`
- `tests/test_backfill_worker.py`
- plus fixtures in `tests/fakes.py`, `tests/conftest.py`.

### Planned concrete modifications
- Add matrix tests for status-code classifier (401/403/404/409/429/5xx).
- Add token refresh race tests for multi-thread contention.
- Add realtime dedupe and retry scheduling tests.
- Add migration-compatibility tests for new SQL columns/defaults.
- Add observability assertions (metrics/log fields emitted).

---

## 9) Migration notes for v2.1 standardization

1. **Schema-first deployment**
   - Apply additive SQL migrations (new nullable columns, new tables, new indexes).
2. **Dual-write period**
   - Application writes both legacy and v2.1 error fields.
3. **Read-switch**
   - Workers read new error taxonomy and retry policies.
4. **Cleanup**
   - Remove legacy interpretation paths after stability window.

### Rollout sequence
- **Dev**: local/integration tests + seeded failure simulation.
- **Staging**: production-like load, replay historical dead-letter sample, validate reconciliations.
- **Prod**: canary worker pool, then full rollout; retain rollback package for one release cycle.

---

## 10) Operational playbook

### Retries
- Retryable classes: network, 429, selected 5xx, explicit transient Zoho codes.
- Backoff: exponential + jitter, cap per attempt tier.
- Respect Retry-After; do not busy-loop on quota exhaustion.

### Alerting
- Page immediately:
  - sustained auth refresh failures,
  - queue growth beyond threshold,
  - realtime lag SLO breach.
- Ticket-only alerts:
  - isolated dead-letter spikes under budget.

### Reconciliation jobs
- Nightly full-module compare (source vs Zoho ID map).
- Hourly delta compare for high-churn entities.
- Auto-open replay tasks for fixable mismatches.

### Observability signals
- Queue depth by status (`NEW/INFLIGHT/DONE/DEAD`).
- Event age percentiles and next-attempt backlog.
- Outbound API success/error/throttle rates by module.
- Token refresh health and expiry headroom.
- Reconciliation drift counts and time-to-repair.
