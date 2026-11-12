# Medline Orders & Confirms Timing — development notes

**Status (2026-10-30): parked.** No code yet and nothing wired into `main.py`.
We're waiting for Medline's final column layout. The feed starts hourly after
the LUM program go-live, currently **2026-11-16** but it may slip.

Short name: **ordertiming** (`--report medline_ordertiming`).

---

## 1. What the sample shows

**File:** `MHS - ORDERS AND CONFIRMS TIMING-<yyyy-mm-dd-hh-mm-ss>.xls`. The
footer calls it "Order and Confirmation Timing v4.0".
**Sender:** `insight@medline.com`. The test copy was forwarded by
`dli2@montefiore.org`, so its subject probably starts with "FW:".
**Sample:** `C:\Users\dli2\Downloads\MHS - ORDERS AND CONFIRMS TIMING-2026-10-29-02-27-13.xls`
(one sheet, `Sheet1`, 10 rows × 22 cols).

### Layout

```
row 0     header (22 columns, below)
rows 1-4  data, one row per (Order #, SAP Delv Doc #)
row 5     SUMMATION: Sold-To # blank;
            ShipToName = "# of Orders:"               Order #         = 3
            Mins  to Order Create = "# of Delivery Docs:"  Mins to Confirm = 4
                                                      Order Lines     = 5
row 6     "Dates & Times Time Zone: Eastern"        <- first cell only
row 7     "Order and Confirmation Timing v4.0"      <- first cell only
row 8     "Date Range: 10/29/2026 to 10/29/2026"    <- first cell only
row 9     2026-10-29 (run date, a real Excel date)  <- first cell only
```

- Rows 6–9 have the same shape as the Fill-Kill and Autosub footers (every
  cell but the first is blank), so the existing shape-based split removes them.
- Row 5, the summation row, does **not** have that shape. Its labels sit in
  data columns and its totals sit in the next column over. It needs its own
  rule (see §2).
- The summation row also gives us a free checksum. In the sample, `# of Orders`
  equals the distinct `Order #` count (3), `# of Delivery Docs` equals the row
  count (4: three delivery docs plus one blank), and `Order Lines` is the sum
  of the column (5).

### Columns (exactly as in the sample, typos included)

| #  | Column                      | Sample                     | Notes |
|----|-----------------------------|----------------------------|-------|
| 1  | `Sold-To #`                 | `0002522116`               | leading zeros, VARCHAR |
| 2  | `Sold-To Name`              | MONTEFIORE WAKEFIELD LAB   | |
| 3  | `Ship-To #`                 | `0002522416`               | leading zeros |
| 4  | `ShipToName`                | LAB CORE                   | no spaces (Fill-Kill uses "Ship-To Name") |
| 5  | `Order #`                   | `1146980830`               | Medline sales order. Same as Fill-Kill `Sales Order #`? |
| 6  | `Channel`                   | EDI                        | |
| 7  | `PO #`                      | `3000566418-STPO`          | same shape as Fill-Kill. Derive `PO` (leading 10 digits) |
| 8  | `EDI Date Recv D`           | date                       | truncated name |
| 9  | `EDI Data Recv Time`        | time                       | "Data" (sic) |
| 10 | `Order Create Date`         | date                       | |
| 11 | `Order Create Time`         | time                       | |
| 12 | `Confim Date`               | date                       | "Confim" (sic) |
| 13 | `Confirm Ti`                | time                       | truncated name |
| 14 | `Mins  to Order Create`     | 2.0                        | **two** spaces after "Mins" |
| 15 | `Mins to Confirm`           | 2.0                        | |
| 16 | `SAP Delv Doc #`            | `8329457185` / blank       | blank = lines not on a delivery yet |
| 17 | `SAP Delv Doc Create Date`  | date / blank               | |
| 18 | `SAP Delv Doc Create Time`  | time / blank               | |
| 19 | `Order Lines`               | 1.0                        | lines on this delivery doc |
| 20 | `ASN Creation Date`         | blank in every sample row  | |
| 21 | `ASN Creation Time`         | blank in every sample row  | |
| 22 | `Branch #`                  | C54                        | |

Calamine reads columns 8–13 and 17–18 as real Excel dates and times.

### Grain: why MERGE is wrong here

Order `1147051160` appears on two rows. One row has no delivery doc (1 line).
The other is on delivery `8329564857` (2 lines). So the grain is
(`Order #`, `SAP Delv Doc #`), and the blank-delivery row covers the part of the
order that isn't on a delivery yet. Later in the day that row should give way to
a row with a real delivery doc, which means **rows can disappear between hourly
files**. Fill-Kill's MERGE never deletes, so it would leave the stale blank row
in the table forever. That's the reason for the day-replace design in §2.

### Time stamps

- The footer says all dates and times are **Eastern**.
- The file name stamp is `02-27-13`, but the data runs to 15:00:10 ET. The
  stamp looks like **Central time on a 12-hour clock** (2:27 PM CT = 3:27 PM
  ET). Autosub stamps are also Central: `06-2x` is the ~7:20 AM ET mail, and
  `12-47` is the 1:50 PM ET resend.
- If the clock really is 12-hour, file names **don't sort chronologically
  within a day** (an 11 AM file sorts after a 2 PM file). The Fill-Kill
  backfill's name sort can't be reused as-is. See Q2.

---

## 2. Planned design

### Names and locations

| Item            | Value |
|-----------------|-------|
| Report key      | `medline_ordertiming` (folder `reports/medline_ordertiming/`, launcher `run_medline_ordertiming.bat`) |
| DB              | `SCSFileIngestor` on O2, assumed to be `YNBBSTVWP02\PROCDATASRVPROD` like the other feeds (Q7) |
| Schema          | `MedlineOrderTiming` (already created) |
| Staging table   | `medlineordertiming_stg`, truncated and reloaded every run |
| Target table    | `MedlineOrderTiming`, latest file per Report Date |
| ETL health      | process_name `Medline Orders & Confirms Timing`, process_id `MEDORDTIMING_H_DL`, frequency `Hourly` |
| Raw / Log / Archived | `\\montefiore.org\centralfiles\data\Procurement PMO\dli2\Medline FillKill\OrderTiming\{Raw,Log,Archived}` (same pattern as Autosub) |

### Load semantics: replace the day, don't MERGE

Each file is a day-to-date snapshot (the footer's Date Range is a single day).
Each run:

1. Downloads the newest matching mail by received time, as Fill-Kill does.
2. Parses the file and takes **Report Date** from the footer line
   `Date Range: mm/dd/yyyy to mm/dd/yyyy`. Fail if start ≠ end until we know
   multi-day ranges really happen.
3. In **one transaction**: `TRUNCATE` staging → insert the rows →
   `DELETE` target `WHERE [Report Date] = @d` → `INSERT` target `FROM` staging.

After that, the target holds exactly the latest file for each day and staging
holds the last file loaded. Reloading the same file is harmless. A zero-row
file (header + summation + footer) correctly empties that day.

This needs a new helper beside `stage_and_upsert` in
[src/staged_upsert.py](../../src/staged_upsert.py), for example
`stage_and_replace(df, conn_cfg, schema, staging_table, target_table,
column_types, key_columns, partition_column)`. It can reuse `ensure_tables`,
`_to_records` and `_q`, and should return staged / deleted / inserted counts.

**Guard: never replace a day with an older file.** A backfill or a delayed run
could otherwise load the 10 AM file after the 3 PM one. Store `source file`
plus a load-order signal, and refuse a file older than the one already loaded
for that day. What the signal should be is still open (Q2): the mail received
time works for the pipeline, and the file stamp works only if it's 24-hour.

### Table shape

- **Columns:** source names stripped, as Fill-Kill keeps them, unless the typos
  survive into the final layout (Q4). Then add `Report Date`, `PO` (leading 10
  digits of `PO #`, for joins to Fill-Kill) and `source file`.
- **Keys:** an identity `ID` surrogate primary key (as Autosub has) plus
  UNIQUE (`Report Date`, `Order #`, `SAP Delv Doc #`). A blank
  `SAP Delv Doc #` is stored as `''` so the key column can be NOT NULL. PBO's
  `MedlineSubCollection` uses the same approach.
- **Types:** IDs as VARCHAR (keeps leading zeros), dates as `DATE`, times as
  `TIME(0)`, `Order Lines` as `INT`. The `Mins …` columns are INT or DECIMAL
  depending on Q5.

### ingestion.py

- `pd.read_excel(engine="calamine", dtype=str)`, strip names and values, and
  validate the column list.
- Split off the footer by shape (rows 6–9) and parse Report Date from the
  "Date Range:" line, as Autosub's `split_footer` / `parse_report_date` do.
- Split off the summation row by its label (`ShipToName == "# of Orders:"`).
  Check its totals against the data (distinct `Order #`, row count, sum of
  `Order Lines`) and **fail on a mismatch**, which catches a truncated
  attachment. If Medline renames the label, the row stays in the data and
  fails the key check (blank `Sold-To #`), so it can't slip in silently.
- Key check: `Order #` must be present and (`Order #`, `SAP Delv Doc #`)
  unique within the file. Fail loudly if not.
- Strict type coercion: an unexpected format fails the run instead of loading
  blanks.

### pipeline.py / config.yaml

- Same shape as Fill-Kill: take the newest matching mail on each run, with
  `inbox_lookback_days: 1`.
- Whether a run with no mail is a quiet skip (`ctx.skip_reason`) or a failure
  depends on Q6.
- **Testing before go-live:** `expected_sender: "dli2@montefiore.org"` (the
  forwarded copy), `keyword: "ORDERS AND CONFIRMS TIMING"` (substring match,
  so "FW:" is fine), `attachment_prefix: "MHS - ORDERS AND CONFIRMS TIMING"`,
  `attachment_extensions: [".xls"]`. **Switch `expected_sender` to
  `insight@medline.com` at go-live.**
- The mailbox is `procurementdatateam@montefiore.org`, the `from_email` every
  feed uses. Confirm the forwarded test copy landed there.

### backfill.py

- Same as Fill-Kill's, except it can't sort by file name if the stamps are
  12-hour (Q2). Group the files by Report Date and load only the latest file
  for each day.

### Wiring checklist

- [ ] `reports/medline_ordertiming/`: `__init__.py`, `ingestion.py`, `persistence.py`, `pipeline.py`, `backfill.py`, `config.yaml` (version log starting at v1.0)
- [ ] `src/staged_upsert.py`: `stage_and_replace` helper
- [ ] `main.py`: REGISTRY entry and docstring
- [ ] `run_medline_ordertiming.bat`: copy of Fill-Kill's (`*.bat` is gitignored)
- [ ] README: update the layout list to five pipelines and add an operational note
- [ ] `--dry-run` backfill against the final sample, then a `--test` run against the forwarded mail
- [ ] Task Scheduler: hourly (hours per Q1), `--prd`, starting on go-live day

---

## 3. Open questions

1. **Day boundary and schedule.** Which hours does Medline send the file
   (around the clock? weekends?)? Does a file after midnight close out the
   previous day, or is activity after the last evening file lost? Which
   column drives the Date Range (EDI receive date? order create date?)? For
   example, where does an order received at 11:55 PM and confirmed after
   midnight land?
2. **File-name stamp.** Is it Central time on a 12-hour clock? Can Medline
   switch to 24-hour? This decides backfill ordering and the "older file"
   guard.
3. **Grain.** Confirm there's one row per (`Order #`, `SAP Delv Doc #`). Can an
   order have more than one blank-delivery row? Can a row move from one day's
   report to another's?
4. **Header names.** If the final layout keeps `Confim Date`, `Confirm Ti`,
   `EDI Date Recv D`, `EDI Data Recv Time` and `Mins  to Order Create` (double
   space), do we keep them as-is (the Fill-Kill convention) or load them under
   clean names? The better fix is asking Medline to correct them.
5. **`Mins to Order Create` / `Mins to Confirm`.** Are they always whole
   minutes? Can they be negative, or blank before confirmation?
6. **Empty hours.** Does Medline still send a file when there are no orders?
   Should a missing hourly mail send a failure email or skip quietly?
7. **Server.** Is "O2" `YNBBSTVWP02\PROCDATASRVPROD`?
8. **Intra-day history.** The DB keeps only the latest file per day (the raw
   files stay on disk). Is an hourly snapshot table ever needed, for example to
   report confirm timing as of a given hour?
