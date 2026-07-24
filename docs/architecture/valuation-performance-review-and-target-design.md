# Valuation & Performance Calculations — Architecture Review and Target Design

**Status:** Proposal
**Scope:** Holdings snapshot pipeline, daily valuation history, performance (TWR/MWR/XIRR) computation, and their SQLite storage footprint.
**Goals:** Reduce end-to-end recalculation time by an order of magnitude on realistic portfolios, and reduce database size by 60–90%, without changing calculation semantics (Decimal precision, flow provenance, valuation quality statuses).

---

## 1. Current Architecture (as-built)

### 1.1 Pipeline

```
activities ──► holdings snapshots (sparse keyframes) ──► daily_account_valuation (dense, 1 row/account/day)
                        │                                          │
                        ├─► lots / lot_disposals (dual-write)      ├─► performance (TWR/MWR/XIRR) — computed on demand, not persisted
                        └─► snapshot_positions (dual-write)        └─► scoped aggregation (ALL / portfolio) — computed on read, in-memory cache
```

1. **Holdings snapshots** (`crates/core/src/portfolio/snapshot/snapshot_service.rs`)
   `recalculate_holdings_snapshots` replays activities day-by-day per account
   (`calculate_daily_holdings_snapshots`, `snapshot_service.rs:946`). A keyframe is
   persisted only for days with activity (plus the first day), so the
   `holdings_snapshots` table is sparse. Each keyframe serializes the **entire**
   account state — every `Position` including its full `lots: VecDeque<Lot>` —
   into the `positions` TEXT column as JSON. Positions are also dual-written to
   the relational `snapshot_positions` table (without lots), and current open
   lots/disposals to `lots`/`lot_disposals`.

2. **Daily valuation** (`crates/core/src/portfolio/valuation/valuation_service.rs:1661`)
   `calculate_valuation_history` first calls
   `get_daily_holdings_snapshots` (`snapshot_service.rs:1209`), which
   **materializes one full snapshot clone per calendar day** by carrying keyframes
   forward. It then values each day with
   `calculate_valuation_with_price_factors` (`valuation_calculator.rs:51`) and
   writes one `daily_account_valuation` row per day (`replace_into`, chunks of
   1000). Cost basis conversion iterates **every lot of every position on every
   day** (`valuation_calculator.rs:218`).

3. **Performance** (`crates/core/src/portfolio/performance/performance_service.rs`)
   Not persisted. TWR (`compute_time_weighted_returns`, line 550), MWR/XIRR and
   attribution are computed per request from the full daily valuation series.

4. **Scoped aggregation** (`valuation_service.rs:1999–2173`)
   "ALL"/portfolio views load **all member accounts' daily rows**, re-derive
   external flows from activities, and merge day-by-day at read time. Results go
   into an in-memory cache keyed by `max(calculated_at)` (invalidated by every
   recalc; the key itself requires a `MAX()` scan per request).

### 1.2 Orchestration

Activity/asset changes flow through the domain-event queue
(`apps/tauri/src/domain_events/queue_worker.rs`, `apps/server/src/domain_events/queue_worker.rs`).
The planner derives `since_date` = earliest changed activity date; the job runs
`SnapshotRecalcMode::SinceDate(d)` then `ValuationRecalcMode::SinceDate(d)`
(`queue_worker.rs:317–324`). App startup / market sync use
`IncrementalFromLast`. Snapshot recalculation processes **all accounts inside a
single sequential day loop**; valuation runs per account via `join_all`
(`apps/tauri/src/listeners.rs:330–342`).

### 1.3 Storage shapes (final schema, `crates/storage-sqlite/src/schema.rs`)

| Table | Granularity | Notable columns |
|---|---|---|
| `holdings_snapshots` | 1 row per account × activity-day | `positions` TEXT (JSON incl. **all lots**), `cash_balances` TEXT |
| `snapshot_positions` | 1 row per keyframe × position | relational dual-write, currently **write-only** ("groundwork, not a read-path switchover") |
| `lots` / `lot_disposals` | current lot state (not per-day) | normalized, indexed |
| `daily_account_valuation` | 1 row per account × **calendar day** | 23 columns, all decimals as TEXT (full `rust_decimal` precision strings) |
| `quotes` | 1 row per asset × day × source | decimals as TEXT; redundant `day` + `timestamp` |

All writes use `replace_into` (SQLite `INSERT OR REPLACE` = delete + insert);
range rewrites delete then bulk-insert inside one transaction.

---

## 2. Review — Findings

### 2.1 Compute hot spots

**P1 — Dense per-day snapshot materialization with full-state clones.**
Both the write path (`calculate_daily_holdings_snapshots`: carry-forward
`previous_holdings_snapshot.clone()` for every no-activity day,
`snapshot_service.rs:1024`) and the valuation path
(`get_daily_holdings_snapshots`: `current_state.clone()` per day,
`snapshot_service.rs:1313`) allocate a deep copy of the whole account state —
every `Position`, every `Lot` (~20 fields each) — for **every calendar day**,
even though holdings only change on activity days. A 10-year account with 100
positions × ~10 lots each ⇒ ~3,650 days × ~1,000 lots ≈ **3.6M deep-cloned Lot
structs** per account per full recalc, before any math happens. This is the
single largest CPU + allocator cost in the pipeline.

**P2 — Per-day per-lot recomputation of values that only change at keyframes.**
`calculate_cost_basis_in_currency` loops over every lot every day
(`valuation_calculator.rs:218`) to redo the same acquisition-FX conversion.
Cost basis, book basis, and quantities are **constant between keyframes**; only
prices and FX vary. The correct complexity is
`O(days × held_assets + keyframes × lots)`; the current one is
`O(days × lots)` with `rust_decimal` arithmetic (10–30× slower than native
floats — appropriate for money, expensive to waste).

**P3 — No real parallelism; repeated data loading per account.**
Snapshot recalculation is one sequential loop over all accounts and days.
Valuation runs per-account futures under `join_all`, but the work is
synchronous CPU + blocking repo calls, so it largely serializes on the runtime;
nothing uses `spawn_blocking`/rayon. Each account's
`calculate_valuation_history` independently re-fetches quotes
(`get_quotes_in_range_filled`) and FX for the same shared assets and date range
— N accounts holding the same ETFs load the same quote series N times per run.

**P4 — Read path re-aggregates and re-parses everything per request.**
Every scoped history request loads all member accounts' daily rows (23 TEXT
columns each, parsed via `parse_decimal_lossy` per field), re-queries
activities to rebuild external flows, and merges in memory. The cache
(`scoped_history_cache`) helps steady-state dashboards but: (a) the key
requires a `MAX(calculated_at)` scan per request; (b) any recalc of any member
account invalidates all scopes; (c) cache entries are full `Vec<DailyAccountValuation>`
clones per (scope × range × mode) — memory grows with each distinct range the
UI asks for.

**P5 — Full-resolution, full-width payloads to the frontend.**
`useValuationHistory` (`apps/frontend/src/hooks/use-valuation-history.ts`)
fetches every daily row with all fields for the selected range (or ALL-time)
with no interval parameter and no client downsampling. A 10-year ALL-time chart
on a 5-account portfolio deserializes ~3.6k aggregated rows × 23 fields across
the IPC/HTTP boundary to draw a ~500-px-wide line.

**P6 — Write churn.**
`replace_into` on `holdings_snapshots` rewrites rows (and their
`snapshot_positions` children are delete+reinserted per snapshot,
`repository.rs:869–896`) even when content is unchanged. Incremental valuation
(`IncrementalFromLast`) recomputes and re-replaces the whole tail from the last
saved date — fine — but a market-sync that adds one new quote day still
re-reconstructs the daily snapshot series from the calculation start.

### 2.2 Database size

**S1 — Lots embedded in every keyframe JSON (dominant cost, superlinear growth).**
Each keyframe re-serializes **all lots accumulated so far**. For an account
that buys monthly into 10 assets for 10 years (120 keyframes, lots growing
10/month to 1,200): Σ lots over keyframes = 10 × (1+2+…+120) ≈ **72,600
serialized Lot objects** at ~600–900 bytes of camelCase JSON each ⇒
**~50–65 MB for one modest DCA account.** An active DRIP/trading account with
weekly activity across 50 positions reaches hundreds of MB. Growth is
O(keyframes × cumulative lots) ≈ quadratic in time for buy-and-hold
accumulation — this is the primary reason databases balloon.

**S2 — Triple representation of the same facts.**
Positions exist as (a) JSON in `holdings_snapshots.positions`, (b) rows in
`snapshot_positions`, (c) implicitly in `lots`. Lots exist in both (a) and the
`lots` table. Only (a) is read today.

**S3 — `daily_account_valuation` is dense, wide, and immortal.**
23 columns × full-precision decimal TEXT (`rust_decimal` serializes up to 28
significant digits, e.g. `1234.5678901234567890123456`) × one row per account
per calendar day, kept forever. 10 accounts × 15 years ≈ 55k rows ≈ 30–60 MB
with indexes. Linear, but the per-row width is ~5–10× what the charts and
performance math actually need at that age.

**S4 — Quotes.** TEXT decimals, `day` + `timestamp` redundancy, and per-source
duplicate rows (`uq_quotes_asset_day_source`). Secondary, but the table is the
largest row-count table in most databases.

### 2.3 What is already good (keep)

- Sparse keyframe model for holdings (only activity days persisted).
- `SinceDate` incremental planning from the domain-event queue — the earliest-
  changed-date logic and the split-restart guard (`snapshot_service.rs:853–872`)
  are sound.
- Deterministic snapshot IDs (`stable_id`) enabling idempotent upserts.
- Normalized `lots`/`lot_disposals`/`snapshot_positions` tables — the target
  design promotes them from dual-write to canonical.
- Flow-provenance and valuation-status semantics (`ExternalFlowSource`,
  `ValuationStatus`) — untouched by this proposal.
- Chunked (1000) transactional bulk writes.

---

## 3. Target Design

Four workstreams, ordered by impact/risk ratio. Each is independently
shippable; together they change complexity from
`O(days × lots)` compute / `O(keyframes × lots)` storage to
`O(days × assets + keyframes × lots)` compute / `O(keyframes × positions)` storage.

### 3.1 WS-A: Make normalized tables canonical; strip lots out of keyframes (DB size)

**Change:**
1. Keyframes stop serializing `lots` inside `positions` JSON. A keyframe stores
   position **aggregates** only (asset_id, quantity, average_cost,
   total_cost_basis, currency, is_alternative, contract_multiplier) + cash
   balances — i.e., exactly what `snapshot_positions` already holds.
2. Switch the snapshot read path to `holdings_snapshots` (metadata + cash) JOIN
   `snapshot_positions`, dropping the `positions` JSON column after migration.
3. Lot-level state as of a historical date, when needed (tax views, per-lot
   cost basis with acquisition FX), is reconstructed from `lots` +
   `lot_disposals` (`original_*` fields + disposal dates already support as-of
   replay). The **latest** lot state — the only one used routinely — is already
   in `lots`.
4. The valuation calculator's per-lot acquisition-FX cost basis
   (`valuation_calculator.rs:218`) reads lot data from the `lots` table once
   per recalc run (keyed by account/asset), not from per-day snapshot clones.

**Effect:** `holdings_snapshots` shrinks from O(keyframes × cumulative lots) to
O(keyframes × positions-touched); the DCA example above drops from ~50–65 MB to
~2–4 MB (>90%). Snapshot serialization/deserialization cost drops
proportionally, which also speeds every recalc and read.

**Migration:** one-time rewrite of `positions` JSON → aggregates-only (or
straight to `snapshot_positions` if the JSON column is dropped in the same
release), then `VACUUM`. Old snapshots deserialise with `#[serde(default)]` on
`lots` already, so a lazy migration (rewrite-on-next-recalc + background sweep)
is also viable.

### 3.2 WS-B: Interval-based valuation compute — never materialize daily snapshots (CPU/memory)

**Change:** Replace `get_daily_holdings_snapshots` + per-day
`calculate_valuation_with_price_factors` with an interval evaluator:

```
for each keyframe interval [k_i, k_{i+1}):          # holdings constant here
    per-interval (once):
        cost_basis, book_basis, net_contribution, cash per currency
        held = [(asset_id, qty, ccy, multiplier)]   # small vector
    per day d in interval:
        investment_value(d) = Σ qty × close(asset,d) × fx(ccy→acct,d)   # O(held assets)
        cash_value(d)       = Σ cash(ccy) × fx(ccy→acct,d)
        emit DailyAccountValuation row (statuses per existing gating rules)
```

- Quotes and FX are loaded **once per recalc run** for the union of all
  accounts' assets/pairs into shared read-only maps
  (`HashMap<(asset, date), close>` / FX matrix), shared across account tasks.
- Per-day work is `O(held assets)` Decimal multiplies; per-lot work happens
  once per interval (and only when lot-level FX cost basis is required).
- The existing quote-gating semantics (full-gap skip, partial-gap
  `PartialUnpriced`, missing-FX error) move into the day loop unchanged.
- Account-level parallelism via `rayon`/`spawn_blocking` (snapshot replay
  stays sequential per account; accounts are independent once transfer
  ordering is handled inside the snapshot phase, which it already is).

**Effect:** eliminates finding P1/P2/P3. For the 10-year × 100-position
example: from ~3.6M lot clones + 3.65M per-lot conversions to ~3.6k interval
setups + ~365k per-asset multiplies — a 10–100× reduction in valuation CPU and
a flat memory profile. Full-portfolio recalc on large books should move from
tens of seconds to ~1s.

### 3.3 WS-C: Read-path — materialized TOTAL series + interval API (latency + payload)

**Change:**
1. During recalculation, after per-account rows are written, upsert a
   **materialized aggregate series** (`account_id = 'TOTAL'`, and one per saved
   portfolio scope if desired) using the existing
   `aggregate_scoped_valuations` logic. Dashboard reads become a single indexed
   range scan; the `MAX(calculated_at)` probe and per-request re-aggregation
   disappear. The in-memory scoped cache remains only as a fallback for ad-hoc
   account combinations.
2. Add `interval: daily | weekly | monthly` to the history queries
   (`get_historical_valuations*`, Tauri command, server route, frontend
   adapter). Downsample in SQL (last-row-per-bucket) so ALL-time charts fetch
   ~120–500 points instead of thousands.
3. Introduce a slim chart DTO — `(date, total_value_base, net_contribution_base,
   currency)` — for the dashboard/networth charts; the full 23-field row stays
   available for the performance page, which needs flows and statuses.

**Effect:** dashboard queries go from `O(accounts × days)` load-parse-merge per
request to one narrow indexed scan; IPC payloads shrink ~10–30×.

### 3.4 WS-D: Storage encoding & retention (DB size, second wave)

1. **Bounded decimal precision at rest.** Round persisted valuation fields to 8
   fractional digits (they are derived, not accounting-of-record values; the
   canonical inputs — activities, lots, quotes — keep full precision).
   Typical row width drops ~40%.
2. **Retention/downsampling policy for `daily_account_valuation`** (off by
   default, user-visible setting): keep daily rows for the trailing N years
   (default 2), end-of-week for years 2–5, end-of-month beyond. TWR over
   coarse buckets remains exact **only if bucket boundaries include every
   external-flow day**, so the compactor must retain all rows where
   `external_inflow_base ≠ 0 ∨ external_outflow_base ≠ 0` (and the row before
   each, for the denominator). Older-period charts and returns are unaffected
   at monthly granularity; anything needing daily resolution for old periods
   can be regenerated on demand via WS-B (recompute is now cheap).
3. **Quotes:** drop the redundant `day`/`timestamp` duplication (keep `day`),
   and apply the same 8-digit rounding on ingest. Optional: retain only the
   highest-priority source per (asset, day) after sync reconciliation.
4. **Housekeeping:** run `PRAGMA auto_vacuum = INCREMENTAL` (or scheduled
   `VACUUM` after migrations/compaction), WAL already implied by the writer.

### 3.5 Expected impact (10-year, 10-account, ~100k-quote reference portfolio)

| Metric | Today (est.) | Target | Driver |
|---|---|---|---|
| `holdings_snapshots` size | 50–500 MB | 2–10 MB | WS-A |
| `daily_account_valuation` size | 30–60 MB | 8–15 MB (no retention) / 3–5 MB (with) | WS-D |
| Full recalc wall time | 10s–2min | ~0.5–3s | WS-B (+A) |
| Activity-edit incremental recalc | seconds | <300 ms | WS-B |
| Dashboard ALL-time history query | 100–800 ms + big payload | <20 ms, ~300 points | WS-C |
| Peak recalc memory | O(days × lots) | O(assets + days) | WS-B |

(Estimates; the Phase 0 benchmark below turns these into measured numbers.)

---

## 4. Migration & Verification Plan

**Phase 0 — Baseline harness (prerequisite for everything).**
- Seeded benchmark DB generator (accounts × years × activity cadence knobs).
- Timing harness around `recalculate_holdings_snapshots` +
  `calculate_valuation_history` + scoped read; `sqlite3_analyzer` (or
  `dbstat`) size report per table. Record baseline.
- **Parity harness:** golden dump of all `daily_account_valuation` rows +
  performance summaries for the seeded DB; every phase must reproduce it
  byte-for-byte (modulo `calculated_at`) before merging. Existing tests
  (`snapshot_service_tests.rs`, `holdings_valuation_service_tests.rs`,
  `current_account_valuation_tests.rs`, performance tests) stay green.

**Phase 1 — WS-B (compute), behind the existing service trait.**
No schema change; `calculate_valuation_history` swaps its internals. Lowest
risk, biggest UX win. Ship with a debug assertion mode that cross-checks the
interval evaluator against the legacy path on `SinceDate` windows.

**Phase 2 — WS-A (lots out of keyframes, snapshot_positions canonical).**
Schema migration + read-path switch + `VACUUM`. Gate with the parity harness
and the existing lot-consistency check (`check_lot_quantity_consistency`).

**Phase 3 — WS-C (TOTAL materialization + interval API + slim DTO).**
Additive; frontend adopts interval param per page.

**Phase 4 — WS-D (precision, retention, quotes).**
Retention shipped as an opt-in setting with a "compact history" action and a
clear explanation of what is kept.

---

## 5. Risks & Open Questions

1. **As-of-date lot reconstruction (WS-A):** tax/holdings views that today read
   historical lots from old keyframe JSON must replay `lots`+`lot_disposals`.
   Audit needed of every reader of `AccountStateSnapshot.positions[*].lots`
   (holdings service, agent tools, addons via type-bridge) before dropping the
   JSON. If an addon-facing API exposes historical lots, keep a compatibility
   shim that reconstructs on demand.
2. **Retention vs. device-sync:** compaction deletes rows; the sync layer must
   treat derived tables (`daily_account_valuation`, snapshots) as
   non-synced/rebuildable — confirm current sync scope before enabling WS-D.2.
3. **HOLDINGS-mode (manual snapshot) accounts:** keyframes are user data, not
   derived — exclude them from any pruning; WS-A applies (their snapshots have
   no lots anyway).
4. **TOTAL materialization staleness (WS-C):** the aggregate row set must be
   rewritten in the same job that rewrites any member account's rows; the
   domain-event queue already serializes portfolio jobs, so this is ordering,
   not locking — but server (multi-writer) mode needs a check.
5. **Precision rounding (WS-D.1):** confirm no consumer does exact-equality
   reconciliation against persisted valuation strings (health checks?) before
   rounding at rest.
