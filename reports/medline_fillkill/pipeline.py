"""Medline Fill-Kill pipeline — hourly incremental load.

Downloads the newest Fill-Kill export from the mailbox, cleanses it, loads it to
the staging table and upserts staging into the Fill-Kill table (see
persistence.py). No report is produced; the returned path is the downloaded raw
file, which the shared runner attaches to the success email (prd sends none
unless notification.success_recipients is filled in). The shared runner
(src/runner.py) provides logging, notification, ETL-health, and maintenance.

For a one-off load of files already on disk (initial history, or re-loading a
missed hour) use ``python -m reports.medline_fillkill.backfill``.
"""

from src.msgraph import get_latest_excel_attachment

from reports.medline_fillkill import ingestion, persistence

# Stages this pipeline reports; the runner adds 3 framing stages
# (config load, success notify, ETL-health) for a total of [1/6]..[6/6].
STEP_COUNT = 3


def run(config, secrets, ctx):
    email_cfg = config["email"]

    # ── 1. Download latest Excel attachment ──
    save_path, latest_file = get_latest_excel_attachment(
        keyword=email_cfg["keyword"],
        destination_path=email_cfg["destination_path"],
        config=config,
        secrets=secrets,
    )
    ctx.source_file_path = save_path
    ctx.progress.step(f"Email attachment downloaded: {latest_file}")

    # ── 2. Ingest, check the primary key & cleanse (a violation raises -> FAIL) ──
    df = ingestion.prepare(save_path)
    ctx.row_count = len(df)
    ctx.progress.step(f"File ingested & cleansed — {len(df)} rows")

    # ── 3. Stage, then upsert into the Fill-Kill table ──
    counts = persistence.stage_and_upsert(df, config["persistence"])
    ctx.progress.step(
        f"Upserted — {counts['inserted']} inserted, {counts['updated']} updated, "
        f"{counts['unchanged']} unchanged"
    )
    return save_path
