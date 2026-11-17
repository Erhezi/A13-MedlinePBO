# Medline Order Timing — development notes

**Status (2026-11-17): built, v1.0; hourly feed live.** Wired into `main.py` as
`--report medline_ordertiming`. The three daily test files (2026-11-10..12) are
loaded. The hourly feed started on 2026-11-17: mails at 07:23, 08:15, 09:37 and
10:11 ET (stamps `06-21-02` .. `09-08-27`, i.e. Central time), each a
day-to-date snapshot (518 -> 1083 -> 1406 -> 1552 lines). The four files are in
`OrderTiming\Raw`, and the 10:11 one is loaded (Report Date 2026-11-17, 1552
lines). **Scheduled** as Task Scheduler "Medline Order Timing Hourly" (`--prd`,
every day hourly at :28, 6:28 AM–11:28 PM, 2 min before Fill-Kill), set up on
the server by `setup_task_scheduler.ps1`. The development PC runs it 7 minutes
later, at :35 (Fill-Kill at :37). How it works for operators is in the README. This file keeps the background and the open questions.

---

## 1. The file

**Name:** `MHS - ORDER DETAIL REPORT-<yyyy-mm-dd-hh-mm-ss>.xls`. The footer title
is "Order Detail with Confirm and ASN Timing" (v4.0).
**Mail:** subject "Insight Report : MHS - ORDER DETAIL REPORT", from
`insight@medline.com`, to `procurementdatateam@montefiore.org`.
**Replaces** the 2026-10-29 "MHS - ORDERS AND CONFIRMS TIMING" sample, which had
one row per (Order #, delivery doc) and 22 columns. The new report has one row
per **order line** and 43 columns, adding item, quantities, bad-line / reject
code, lot and expiration.

### Layout

```
header      43 columns
data        one row per order line; (Order #, Order Line) unique
blank
totals      "Grand Totals", "# of PO's:", "# of Medline Orders:", "# of Order Lines:",
            "# of Bad / Rejected Lines:", "EDI 850 PO Count", "EDI 855 Confirm Count",
            "EDI 856 ASN Count"  — label in Sold-To Name, value in Ship-To #
blank
footer      "Dates & Times Time Zone:" / title / "Date Range:mm/dd/yyyy to mm/dd/yyyy" /
            run date — first cell only
```

On the 257-line 2026-10-07 sample
(`Downloads\Order Detail with Confirm and ASN Timing  4_0 (12).xls`):

- Lines = 257 = rows; POs = distinct `PO#`; orders = distinct `Order #`.
- Bad / rejected = count of `Bad Line = YES`. `Rej Code` is E1 / S1 / X1 on the same rows, which always have Shipped 0 and no delivery doc.
- EDI 850 = distinct POs, 855 = orders with an `855 Date`, 856 = orders with an ASN.

The ingest checks the first four of these totals. Only the line count fails the
run.

### Formats

- Most dates and times are real Excel values. But `SAP Delv Doc Create Time` and `ASN Creation Time` are text (`7:30:18AM`), and `ASN Creation Date` can be text (`10/07/2026`). `format="mixed"` parses each cell on its own.
- File stamps (`01-04-01`, `01-09-01`, `01-07-19`) arrived at about 2 PM ET. That fits **Central time on a 12-hour clock**, so names can't order one day's files.
- Delivery can lag: the 11-12 file (stamp `01-07-19`) reached the Inbox at 06:39 ET on 11-13. The load still lands on the right day, because the Report Date comes from the footer, not the received time.

## 2. Design decisions

- **Day-replace, not MERGE** (`src/staged_upsert.stage_and_replace`). Each run deletes the file's Report Date and inserts the file, in one transaction. Lines that change or drop off during the day stay current. The target has `load stamp` instead of create / update stamps.
- **Key** (Report Date, Order #, Order Line), with no surrogate ID, since IDs would churn on every hourly reload.
- **No hard "older file" guard.** The pipeline always takes the newest mail by received time. If a day shrinks, the run logs a WARNING and an ETL-health warning but still loads. The backfill loads only the fullest file per day.
- **Freshness warning, not failure.** If the newest mail is over 90 min old (`email.max_mail_age_minutes`), the run still loads it and logs SUCCESS with "Warning: no new file in N min" in the ETL-health `Error` column. Fill-Kill uses the same rule (`src/msgraph.stale_mail_warning`). The test phase (one file a day) warns on every run, and so will the first run of each morning if Medline doesn't send overnight.
- **Raw files are the intra-day history.** Each hourly download keeps its own stamped name in `OrderTiming\Raw`. Monthly maintenance moves last month's files to `OrderTiming\Archived\<yyyy>\<mm>` (`maintenance.archive_by_month`, also on for Fill-Kill). The month comes from the date in the name, not the modified time, because the newest mail is re-downloaded every run, so a day's last file is saved again the next morning.
- **Column names as-is**, typos included (Fill-Kill convention).
- **Shared match keys** in `src/medline_keys.py`: `PO`, `POLine` and `ShipTo`, the same rule in Fill-Kill. `POLine` is `VARCHAR(5)` text without leading zeros, the format of `POLine` in PLMPreprocessorShared's `infor.PURCHASEORDER_LINE` (digits, no zeros). Fill-Kill v1.1 gained `ShipTo`, and its `POLine` changed from `INT` to text. `ensure_tables` now adds missing columns to existing tables, and the 291 old Fill-Kill rows were back-filled once by SQL. Since 2026-11-17 (Order Timing v1.1, Fill-Kill v1.3) both feeds also carry `SoldTo` (sold-to without leading zeros), and both store the order line without leading zeros (`Order Line` here, `Order Line #` in Fill-Kill), so the feeds join line to line. The 2623 Order Timing rows already loaded were fixed once by SQL (Order Line stripped, SoldTo filled, `load stamp` untouched).

## 3. Open questions

1. **Day boundary and hours.** Which hours will Medline send after go-live, and on weekends? What drives the Date Range (Order Create Date in every sample)? Does a line created at 11:55 PM ever show up in the next day's report?
2. **No mail in the last day** fails the run and sends an email, as Fill-Kill does. During the test phase (one file a day, none on weekends) that means a failure on Monday mornings. Decide skip-vs-fail once the hourly cadence is known.
3. **File stamp.** Can Medline switch to a 24-hour clock? Then the backfill could order a day's files by name.
4. **Intra-day history.** The DB keeps only the latest file per day; the raw files stay in `OrderTiming\Raw`, then `Archived\<yyyy>\<mm>`. Is an hourly snapshot ever needed in the DB?

## 4. Go-live checklist

- [x] Package, config v1.0, `main.py` entry, `run_medline_ordertiming.bat` (tracked since 2026-11-17)
- [x] Backfill of the 2026-11-10..12 test files; pipeline step run against the 11-12 mail
- [x] `--test` run through `main.py` once a same-day file is in the Inbox (2026-11-17 10:17, via `run_medline_ordertiming.bat --test`, which now falls back to `.venv\` like the Fill-Kill launcher)
- [x] Task Scheduler "Medline Order Timing Hourly": `--prd`, every day hourly at :28, 6:28 AM–11:28 PM on the server (`setup_task_scheduler.ps1`), :35 on the development PC (registered 2026-11-17; revisit the hours once Q1 is answered)
