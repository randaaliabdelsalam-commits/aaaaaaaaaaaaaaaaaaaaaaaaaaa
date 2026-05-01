# Zoho API Research for Sync Platform (v2.1 Standardization)

## 1) URL Map: Required Endpoints + Child URLs from API Menu Tree

### 1.1 Core auth and regional base URLs
- OAuth token refresh endpoint (US DC): `https://accounts.zoho.com/oauth/v2/token`
- CRM API base (US DC): `https://www.zohoapis.com/crm/v8`
- CRM API base (IN DC): `https://www.zohoapis.in/crm/v8`
- Creator API base (legacy/current in codebase): configurable `api_base` in `sync/zoho_client.py`.

> Note: our current workers call **Zoho Creator-style form/report endpoints** via `ZohoClient` (`/{owner}/{app}/form/{form}` and `/{owner}/{app}/report/{report}/{record_id}`), while this research also captures CRM v8 endpoints needed for v2.1 standardization.

### 1.2 API menu-tree roots (CRM v8)
- API references index: `https://www.zoho.com/crm/developer/docs/api/v8/api-references.html`
- Modules metadata: `https://www.zoho.com/crm/developer/docs/api/v8/modules-api.html`
- Module metadata: `https://www.zoho.com/crm/developer/docs/api/v8/module-meta.html`
- Field metadata: `https://www.zoho.com/crm/developer/docs/api/v8/field-meta.html`
- Bulk Read (create/status/download):
  - `https://www.zoho.com/crm/developer/docs/api/v8/bulk-read/create-job.html`
- Bulk Write (create/upload/status/result):
  - `https://www.zoho.com/crm/developer/docs/api/v8/bulk-write/create-job.html`
  - `https://www.zoho.com/crm/developer/docs/api/v8/bulk-write/limitations.html`

### 1.3 Discovered child operation URLs to track in integration docs
#### Record CRUD + lifecycle
- Insert records: `POST /{module}`
- Update records: `PUT /{module}`
- Upsert records: `POST /{module}/upsert`
- Delete records: `DELETE /{module}`
- Get records: `GET /{module}`
- Get record by id: `GET /{module}/{record_id}`

#### Metadata and schema discovery
- List modules: `GET /settings/modules`
- Single module metadata: `GET /settings/modules/{module_api_name}`
- Module fields: `GET /settings/fields?module={module_api_name}`

#### Publish / eventing surfaces
- Notifications (enable/get/disable channel): `/actions/watch`
- Webhooks and signal-based ingestion should map to queue producers feeding `SYNC_EVENTS`.

#### Bulk surfaces
- Bulk read create job: `POST /bulk/read`
- Bulk read job details: `GET /bulk/read/{job_id}`
- Bulk read result download: `GET /bulk/read/{job_id}/result`
- Bulk write create job: `POST /bulk/write`
- Bulk write upload: `POST /bulk/write/{job_id}`
- Bulk write details: `GET /bulk/write/{job_id}`
- Bulk write successful records: `GET /bulk/write/{job_id}/successful_results`
- Bulk write failed records: `GET /bulk/write/{job_id}/failed_results`

---

## 2) Authentication model (implementation guidance)

- Current implementation uses refresh-token OAuth and stores refreshed `access_token` and `expires_in` in environment file state.
- Token refresh should remain single-flight/thread-safe.
- v2.1 standardization target:
  - Persist `expires_at` as absolute epoch (avoid relative-value drift).
  - Add DC-aware auth host selection (`accounts.zoho.{com|in|eu|...}`) derived from config.
  - Treat invalid_grant / revoked refresh token as **non-retryable** and page immediately.

---

## 3) Status-code / error semantics

### 3.1 Currently handled in runtime
- Success: `200`, `201`
- Unauthorized: `401` => force token refresh + retry.
- Rate-limit signals: HTTP `429` and Creator code `2955` => retryable + limiter slowdown.
- Daily quota signal: Creator code `4000` => retryable with long delay.
- Server errors: `5xx` => infinite retry with connectivity-aware backoff.
- Other `4xx` => fail fast (`ZohoError`) and dead-letter after attempts.

### 3.2 Standardized target table for v2.1
- `2xx`: success
- `400/422`: payload or mapping defect, non-retryable, route to dead/reconciliation.
- `401`: refresh token and retry once per attempt cycle.
- `403`: auth scope/permission issue, non-retryable until config fix.
- `404`: missing module/report/record; treat as mapping/config fault.
- `409`: conflict; retry only for known idempotent upserts.
- `429`: retryable with Retry-After honored.
- `5xx`: retryable with capped exponential backoff + jitter.

---

## 4) Add / Update / Delete API patterns

- **Add**: construct normalized payload envelope `{ "data": ... }`, capture returned Zoho ID, persist in `ZOHO_MAP`.
- **Update**: lookup mapped Zoho ID first; if absent, fallback to add (idempotent create path).
- **Delete**: only execute when mapping exists; remove mapping row upon successful delete.
- **Idempotency rule**: for duplicate realtime events, operations must converge on one mapped Zoho record per `(module, key tuple)`.

---

## 5) Fields metadata strategy

- Fetch and cache module + field metadata daily (or on deploy).
- Validate payload keys against metadata API names before enqueue or before outbound write.
- Store metadata snapshots (JSON) for traceability of drift and historical debugging.
- For strict mode rollout, reject unknown field names early and emit actionable dead-letter reasons.

---

## 6) Publish APIs and event transport

- Use watch/notification channels (where CRM module supports it) for near-realtime triggers.
- Convert external callbacks into `SYNC_EVENTS` in a normalized schema.
- Include dedupe key (`source_table/op/key/timestamp bucket`) to avoid duplicate work storms.

---

## 7) Bulk API strategy

- **Bulk Read**: baseline and reconciliation exports (nightly / hourly deltas).
- **Bulk Write**: controlled backfills or mass remediations, not first-line realtime path.
- Chunking guideline: split files/jobs to stay inside documented row/file/job limits.
- Always poll job status and ingest failed rows into reconciliation workflow.

---

## 8) Limits and throttling

- Respect per-org and per-edition API quotas.
- Honor `Retry-After` when present.
- Keep adaptive limiter (`ZohoTrafficGate`) with dynamic slowdown on throttle codes.
- Reserve capacity for realtime path over backfill/dead-letter replays via priority lanes.

---

## 9) v2.1 migration notes + rollout sequence

### Scope of v2.1 standardization
1. Unified error taxonomy (retryable vs non-retryable) across all workers.
2. DC-aware OAuth/API URL config.
3. Metadata preflight validation for fields.
4. Replay-safe idempotency and reconciliation automation.

### Rollout sequence
1. **Dev**
   - Enable verbose request/response classification logs.
   - Run synthetic fault tests (401/429/5xx/network flap).
   - Validate SQL migration forward/backward in isolated DB.
2. **Staging**
   - Shadow-run reconciliation jobs against production-like data volume.
   - Burn-in retry/limiter behavior for at least 72 hours.
   - Verify alert thresholds with controlled failure injection.
3. **Prod**
   - Deploy SQL migrations first.
   - Deploy token/client changes.
   - Enable metadata enforcement in warn-only mode, then strict mode.
   - Monitor SLOs and error budgets for first 7 days.

