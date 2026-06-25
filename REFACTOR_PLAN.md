# Multi-Report Refactor Plan — PBO + Allocation

**Decision (2026-06-24):** Scope is **exactly two reports** (Medline PBO + Medline Allocation).
Keep abstraction minimal: extract only what is *provably identical*, **no** config-inheritance
(`base.yaml`/deep-merge), duplicate the handful of shared config values across the two config files.
**STATUS — IMPLEMENTED on branch `medline_alloc` (2026-06-25).** See §8 "As-built" for
what was actually done and where it deviated from the original plan below. The PBO
behavior-preserving gate (Phase 1) still needs a live run to confirm byte-identical output.

---

## 1. Target layout

```
A13-MedlinePBO/
  pbo_core/                     # shared, report-agnostic library (rename of today's src/)
    __init__.py
    config_loader.py            # MOVE verbatim
    secret_crypto.py            # MOVE verbatim
    logger.py                   # MOVE verbatim
    maintenance.py              # MOVE verbatim (already config-driven)
    msgraph.py                  # MOVE; already config-driven (fetch + notify). notify becomes optional.
    db.py                       # get_connection + scs_engine() + insert_etl_health + fetch_tables(conn, locations, templates)
    queries.py                  # BASE_TEMPLATES = the 6 identical SQL strings
    inventory.py                # the SHARED enrichment kernel carved out of transform.py
    excel.py                    # generic styler carved out of report.py, driven by config keys
    runner.py                   # harness carved out of main.py worker_main (timeout/log/notify/etl-health/maintenance)

  reports/
    medline_pbo/
      __init__.py
      config.yaml               # MOVE today's ./config.yaml here (+ new styler keys, see §4)
      queries.py                # uses BASE_TEMPLATES as-is
      ingestion.py              # read_pbo_file (skiprows=1), validate_columns, apply_jesse_selection (PBO rule), extract_uom_table
      transform.py              # PBO-only: get-well overlays, UOM-alternatives, assemble_output
      pipeline.py               # run(config, secrets) -> output_path   (today's 8-step worker body)

    medline_allocation/
      __init__.py
      config.yaml               # NEW — written from the notebook constants
      queries.py                # BASE_TEMPLATES + collect_item + lost_alloc
      ingestion.py              # read (.xls, no skiprows), validate, remove_footer, apply_jesse_selection (C-group/date rule), make_year_week, clean_int_cols
      persistence.py            # the two upserts (WeeklyAllocHelper, WeeklyAllocUNSPSC) via pbo_core.db.scs_engine()
      transform.py              # alloc-only: VendorItem->Item, EA-by-MPN, risk-of-loss grid, recommendations, prior-weeks concat
      pipeline.py               # run(config, secrets) -> output_path

  registry.py                   # {"medline_pbo": (cfg_path, pipeline.run), "medline_allocation": (...)}
  main.py                       # dispatcher: python main.py --report medline_pbo
  first_time_setup.py / decrypt_env.py / encrypt_env.py / .env.enc / requirements.txt   # keep
```

**Rename note:** `src/` → `pbo_core/` is a one-shot find/replace of `from src.` → `from pbo_core.`.
If you'd rather avoid even that churn, keep the package named `src/` — purely cosmetic, decide at execution time.

---

## 2. The shared-core seam (function-level, verified against transform.py)

Not every inventory-side function is fully shared. PBO overlays a **"Get Well"** layer that depends on
PBO-only input columns (`Est Available Date`, `Est Depletion Date`) which Allocation does not have. So the
honest split is:

| transform.py function | Shared kernel → `pbo_core/inventory.py` | PBO-only overlay → `reports/medline_pbo/transform.py` |
|---|---|---|
| `_aggregate_inventory` / `_aggregate_usage` / `_aggregate_plmusage` | **whole thing** (identical aggregation both reports) | — |
| `prepare_location_inventory_tables` | aggregation + `In IPYCSTRM` flag | **UOM-inconsistency detection** + `ipyc_items` packaging stay PBO |
| `build_ehc` | **whole thing** | — |
| `build_substitutes` | **whole thing** | — |
| `merge_inventory` | **whole thing** | — |
| `calculate_dioh_metrics` | raw `DIOH` (0-guard) + `Matched IMDC + IPYC` + `No Issue Out` | **`Days to Get Well` / `Get Well - DIOH`** (uses Est Available Date + review_threshold) |
| `merge_substitutes` | **whole thing** | — |
| `aggregate_item_groups` | availability-by-IG + `DIOH_ig` | **`Get Well - DIOH_ig` / `Flag_ig`** (PBO get-well variant) |
| `build_full_dataset` | **whole thing** | — |
| `calculate_review_recommendations` | — | **PBO** (Allocation has its own with the allocation cap) |
| `build_uom_conversions` / `apply_uom_alternatives` | — | **PBO only** (Allocation has no packaging-string path) |
| `assemble_output` | — | **PBO** (ipyc_items + timestamp specific) |

**Why this is safe:** the kernel operates only on the *inventory vocabulary* both reports share verbatim
(`Item`, `AvailableQty`, `OnOrderQty`, `DefaultBuyUOM`, `BuyUOMMultiplier`, `StockUOM`,
`AverageDailyIssueOut`, `DIOH`, `Item Group`, `Replace Item`, `_Repl.`, `DIOH_ig`). The `Flag` /
"Review" decision is **per-report** (PBO: get-well vs review_threshold; Allocation: `DIOH_final < 21`) —
do **not** try to unify it.

### Allocation-only logic (stays in `reports/medline_allocation/transform.py`)
- `df_collect_item` join: resolve `Item` from `Material #` (a VendorItem) — Allocation's keys differ from PBO.
- EA conversions by MPN: `Weekly Allocation Qty (EA[, by MPN])`, `Qty available to order (EA[, by MPN])`.
- Risk-of-loss grid: the multi-week `MultiIndex` + `ffill` + `shift(1..3)` + `min` (notebook cells `e2d495`, `8a7a95`, `10f3d8`).
- Recommendations with allocation cap: `Recommended Order Qty with Allocation (...)`.
- Prior-weeks concat into the final frame before styling.

---

## 3. Two non-obvious things Allocation forces

1. **Two DB connections, not one.** Allocation reads `lost_alloc` back from `SCSFileIngestor`
   (its own helper table), while inventory comes from `PRIME`. Keep this **explicit in `pipeline.py`**
   (`fetch_tables(prime_conn, …)` + a separate `scs_conn` read) rather than generalizing `fetch_tables`
   to multi-connection. `pbo_core/db.py` exposes a `scs_engine()` / `scs_connection()` helper; the
   **upsert DML stays in `reports/medline_allocation/persistence.py`** (it's table-specific).

2. **Allocation is stateful — order of operations matters.** The pipeline must
   **upsert this week → then read history → then build the grid**:
   `fetch email → ingest → persistence.upsert(WeeklyAllocHelper, WeeklyAllocUNSPSC) → read lost_alloc (incl. current week) → transform → style`.
   This is why the sequence lives in Python (`pipeline.py`), not config.

---

## 4. Config-schema additions (the styler keys) + values to duplicate

`pbo_core/excel.py` becomes generic by reading these from each report's `config.yaml` (today they're
hardcoded in each styler). Set PBO's to its **current** values so Phase 1 output is unchanged.

```yaml
report:
  # ── styler (new) ──
  font: { name: "Calibri", size: 10 }          # Allocation: { name: "Arial", size: 8 }
  sort_by:        ["Matched IMDC + IPYC", "Get Well - DIOH", "Get Well - DIOH_ig"]   # Allocation: ["Dummy ID"]
  sort_ascending: [true, false, false]                                               # Allocation: [true]
  row_filter:     { column: "Matched IMDC + IPYC", value: "Matched" }   # Allocation: { column: "YearWeek", value: "<current>" }
  highlight:      null                          # Allocation: { driver_col: "Jesse Selection", driver_value: "x",
                                                #               target_cols: ["Allocation Period Start Dt", ...] }
  header_colors:                                # group name -> hex; Allocation adds base_report_derived: "#B7D9ED"
    base_report: "#94c5e3"
    item:        "#003769"
    item_group:  "#112b47"
    rmd:         "#ca006c"
  conditional_color:                            # PBO's red/green on Get-Well; Allocation: null
    columns: ["Get Well - DIOH", "Get Well - DIOH_ig"]
    threshold_key: review_threshold
  extra_sheets:   ["uom_inconsistency"]         # PBO only; Allocation: []  (see gotcha #5)
```

**Values to duplicate across both `config.yaml` files** (no inheritance, by decision):
`email.aad_endpoint`, `email.graph_endpoint`, `email.from_email`; `database.driver`,
`database.trusted_connection`, `database.locations`; the `etl_health.*` connection block. Each report
still differs on `email.keyword`, `email.destination_path`, `report.output_dir`, column groups, and
thresholds.

---

## 5. Gotchas / risks (capture now, don't get surprised later)

1. **PBO is in production (Tue/Thu, ETL-health tracked).** Refactor it **behavior-preserving first**,
   diff the output, *then* add Allocation. See gate in Phase 1.
2. **Plaintext secret in the notebook** (`MedlineAllocation.ipynb` cell `f89425db`: `CLIENT_SECRET = "2Q98Q~…"`).
   Same service account PBO already encrypts → reuse `load_secrets`/`.env.enc` and delete the literal.
   **If this notebook was ever committed/pushed, rotate the secret.**
3. **Reader differences are real:** Allocation reads `.xls` with `engine="calamine"`, `dtype=str`, **no**
   `skiprows`; PBO reads `.xlsx` with `skiprows=1`. Keep `read_*` per-report (already planned).
4. **Allocation has no notification today** (just saves a file). `pbo_core/runner.py` must treat
   success-notify as **optional** (skip when `notification.success_recipients` is empty). Failure-notify +
   ETL-health it can gain for free — confirm you want those (recommended).
5. **Second worksheet:** PBO writes a `UOM inconsistency` sheet; Allocation writes none. `excel.py` writes
   the main `Full` sheet and takes an **optional** list of extra-sheet writers (keep PBO's as a small
   PBO-side callback) rather than baking it into the shared engine.
6. **Closure captures to clean up** when lifting Allocation's styler: it references `current_yearweek`,
   `base_report_derived`, and `target_cols` from enclosing scope — these become config + a passed-in
   `filter_value`.
7. **Dependencies:** Allocation needs `sqlalchemy` (upserts). Verify it's in `requirements.txt`
   alongside `python-calamine`, `xlsxwriter`. Add if missing.
8. **Schedules differ:** PBO Tue/Thu; Allocation is weekly (YearWeek + prior-weeks). Each report keeps its
   own `.bat` / scheduled task, both calling `python main.py --report <name>`.

---

## 6. Phased execution (checklist)

### Phase 0 — Safety net
- [ ] New branch.
- [ ] Capture a known-good PBO run: keep one real input file + the exact `Processed_Monte_PBO …xlsx` it
      currently produces, to diff against after the refactor.

### Phase 1 — Carve out `pbo_core/`, **zero behavior change to PBO**
- [ ] Move `config_loader.py`, `secret_crypto.py`, `logger.py`, `maintenance.py` unchanged.
- [ ] Split `db.py`: keep `get_connection`/`insert_etl_health`; add `scs_engine()`; move SQL to
      `queries.py` as `BASE_TEMPLATES`; make `fetch_tables(conn, locations, templates=BASE_TEMPLATES)`.
- [ ] Carve `inventory.py` kernel per the §2 table (move the "whole thing" rows verbatim; split the two
      `calculate_dioh_metrics` / `aggregate_item_groups` functions into core-kernel + PBO-overlay).
- [ ] Extract `excel.py`; add the §4 keys; set PBO `config.yaml` to current hardcoded values.
- [ ] Extract `runner.py` from `worker_main`; PBO body becomes `reports/medline_pbo/pipeline.py:run`.
- [ ] Add `registry.py` + new `main.py` dispatcher; move `config.yaml` → `reports/medline_pbo/`.
- [ ] **GATE:** `python main.py --report medline_pbo` output **diffs identical** to the Phase-0 capture.

### Phase 2 — Add `reports/medline_allocation/`
- [ ] Write `config.yaml` from the notebook constants (required/int cols, column groups incl.
      `base_report_derived`, `target_dioh: 21`, keyword `"Product Allocation Report"`, paths,
      prior-weeks window `3`, styler keys from §4).
- [ ] `queries.py` = `BASE_TEMPLATES` + `collect_item` + `lost_alloc` (the latter `.format`-ed with `prior_3_week`).
- [ ] `ingestion.py`: read/validate/remove_footer/jesse(C-group+date)/make_year_week/clean_int_cols.
- [ ] `persistence.py`: the two upserts via `scs_engine()`; secrets from `.env.enc`, literal deleted.
- [ ] `transform.py`: VendorItem→Item, EA-by-MPN, risk-of-loss grid, recommendations, prior-weeks concat;
      call shared `inventory.py` for EHC/substitute/item-group/DIOH.
- [ ] `pipeline.py`: enforce **upsert-before-read-history** order (§3.2).
- [ ] Decide Allocation's notify/ETL-health (recommend: failure-notify + ETL-health on, success-notify off).
- [ ] **GATE:** `python main.py --report medline_allocation` reproduces the notebook's xlsx for the same input week.

### Phase 3 — Cleanup
- [ ] Archive/delete the two `.ipynb` files (keep one tagged copy as reference if you like).
- [ ] Update `README.md`, `requirements.txt`, and the scheduled-task `.bat`(s) to the `--report` form.

---

## 7. Decisions (resolved 2026-06-24)
1. **Keep `src/`** as the shared package name (no rename to `pbo_core/`).
2. **No registry/dispatcher** — two separate entry scripts (`run_medline_pbo.py`,
   `run_medline_allocation.py`), scheduled independently. (`main.py` → `run_medline_pbo.py`.)
3. **Allocation gets the full treatment**: its own failure notification, ETL-health log,
   **and** success notification (report emailed; recipient list copied from PBO). Its
   folder tree mirrors Monte PBO under `\\…\dli2\Medline Allocation`.
4. **Notebooks** moved to `notebook/` (still git-ignored, "no publishing"), with a
   prototyping-only note; secret redacted from the notebook on disk.

## 8. As-built (branch `medline_alloc`)

What actually got built, and where it deviates from §1–§6 above:

- **Shared `src/`** (kept the name): `queries.py` (BASE_TEMPLATES), `db.py` (+`get_scs_connection`,
  `get_scs_engine`, generic `fetch_tables`), `msgraph.py` (attachment prefix/extensions/lookback
  + notification `report_name` now config-driven), `maintenance.py` (archive glob config-driven),
  `excel.py` (NEW config-driven styler replacing `report.py`), `runner.py` (NEW entry harness from `main.py`).
- **`reports/medline_pbo/`**: `ingestion.py` + `transform.py` (moved verbatim), `pipeline.py` (the
  8-step worker body), `config.yaml` (moved + new styler/msgraph/maintenance keys at current values).
- **`reports/medline_allocation/`**: `config.yaml`, `queries.py`, `weeks.py`, `ingestion.py`,
  `persistence.py` (the two upserts), `transform.py`, `pipeline.py` — ported from the notebook.
- **Entry scripts**: `run_medline_pbo.py`, `run_medline_allocation.py`; `.bat` launchers updated/added.
- **Deps**: `SQLAlchemy` (+`greenlet`) added to `requirements.txt` for the Allocation upserts.

**Deviation from the plan — no shared `inventory.py`.** Deeper reading showed the two transform
chains are *column-divergent* (PBO carries `VendorItem`/`report stamp`/`Est *` through the chain;
Allocation doesn't, and keys off `Material #`→`Item`). Per the "extract only provably identical"
directive, forcing a parameterized shared engine across two reports was not worth the coupling/risk,
so each report keeps its own self-contained `transform.py`. The real, clean sharing is the
infra + styler + runner. Revisit a shared inventory engine only if a **third** report appears.

**Validation done offline** (no DB/network here): byte-compile + imports for the whole tree; config
wiring through the styler for both reports; PBO output filename reproduced exactly
(`Processed_Monte_PBO (v1.5) …`); Allocation filename matches the notebook
(`Processed_Medline_Product_Allocation (v1.3)-…`); and a synthetic end-to-end styler run for both
configs produced valid workbooks (PBO `Full`+`UOM inconsistency`; Allocation `Full`).

**Still requires a live run (Phase 1 gate):** confirm `python run_medline_pbo.py` produces a report
**byte-identical** to a pre-refactor baseline before relying on it in production. Also create the
`[MedlineAllocation].[ETLHealth]` table (same shape as `[MedlinePBO].[ETLHealth]`) and rotate the
Microsoft Graph client secret (it was never committed to git, but lived in a OneDrive-synced notebook).
