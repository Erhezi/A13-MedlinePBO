"""Medline Order Detail (orders & confirms timing) pipeline — hourly day-replace load.

Each run downloads the newest "MHS - ORDER DETAIL REPORT-<stamp>.xls" mailed in
the last ``email.inbox_lookback_days``, checks and cleanses it, and replaces that
file's Report Date in the target table with its lines (see persistence.py).
Within a day each file holds the day's lines so far, so the latest file wins;
the first file of a new day starts a new Report Date. Re-loading the same file
on a later run is harmless.

Medline has sent the file hourly since 2026-11-17 (one test file a day
before that); the run is scheduled hourly at :28. No report is produced; the
returned path is the downloaded raw file. The shared runner (src/runner.py) provides logging,
notification, ETL-health, and maintenance.

For files already on disk use ``python -m reports.medline_ordertiming.backfill``.
"""

from src.msgraph import download_latest_excel_attachment, stale_mail_warning

from reports.medline_ordertiming import ingestion, persistence

# Stages this pipeline reports; the runner adds 3 framing stages
# (config load, success notify, ETL-health) for a total of [1/7]..[7/7].
STEP_COUNT = 4


def run(config, secrets, ctx):
    email_cfg = config["email"]

    # ── 1. Download latest Excel attachment (warn if Medline sent nothing new lately) ──
    found = download_latest_excel_attachment(
        keyword=email_cfg["keyword"],
        destination_path=email_cfg["destination_path"],
        config=config,
        secrets=secrets,
    )
    save_path = found.save_path
    ctx.source_file_path = save_path
    stale = stale_mail_warning(found, email_cfg.get("max_mail_age_minutes"))
    if stale:
        print(stale)
        ctx.warnings.append(stale)
    ctx.progress.step(f"Email attachment downloaded: {found.file_name}")

    # ── 2. Ingest, check totals & keys, cleanse (a violation raises -> FAIL;
    #       a "# of Order Lines" mismatch is a warning, emailed as an alert) ──
    prepared = ingestion.prepare(save_path)
    df, report_date = prepared.rows, prepared.report_date
    ctx.row_count = len(df)
    ctx.warnings.extend(prepared.warnings)
    ctx.alerts.extend(prepared.alerts)
    ctx.progress.step(f"File ingested & cleansed — {len(df)} lines, Report Date {report_date}")

    # ── 3. Stage, then replace the Report Date in the target ──
    counts = persistence.replace_day(df, report_date, config["persistence"])
    if counts["inserted"] < counts["deleted"]:
        ctx.warnings.append(
            f"Warning: Report Date {report_date} shrank from {counts['deleted']} to "
            f"{counts['inserted']} lines"
        )
    ctx.progress.step(
        f"Report Date {report_date} replaced — {counts['deleted']} old line(s) deleted, "
        f"{counts['inserted']} inserted"
    )

    # ── 4. Upsert each lot shipped into MedlineLot ──
    lot_counts = persistence.upsert_lots(prepared.lots, config["persistence"])
    ctx.progress.step(
        f"Lots upserted — {lot_counts['staged']} lot(s): {lot_counts['inserted']} inserted, "
        f"{lot_counts['updated']} updated, {lot_counts['unchanged']} unchanged"
    )
    return save_path
