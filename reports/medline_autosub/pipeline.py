"""Medline Daily Autosub Management pipeline — daily load, scheduled several times a day.

Medline mails the report around 7:20 AM and sometimes re-sends a fuller version
later the same day (2026-10-27: a 1:50 PM resend with 61 rows vs 35). The task
runs every 4 hours on weekdays (7:50 AM, 11:50 AM, 3:50 PM) and each run:

  1. looks up today's last SUCCESS for this process in ETL health;
  2. downloads the newest export received since that load started — or since
     local midnight if there is none, so a run before the mail arrives can never
     load yesterday's file and mark the day done. No such mail means: already
     loaded today -> skip (nothing newer); not loaded and still before
     ``email.expected_by`` -> skip quietly; not loaded after it -> fail (and email);
  3. unpivots AS1..AS4 into one row per (PRIMARY ML #, AutoSub); a repeated key
     keeps its first row in file order;
  4. upserts the daily table and 5. the (PRIMARY ML #, AutoSub) relation table
     (see persistence.py).

A skipped run sends no email and writes no ETL-health row (see runner.RunContext).
No report is produced; the returned path is the downloaded raw file. For files
already on disk use ``python -m reports.medline_autosub.backfill``.
"""

from datetime import date, datetime, time

from src.db import last_etl_success_since
from src.msgraph import get_latest_excel_attachment

from reports.medline_autosub import ingestion, persistence

# Stages this pipeline reports; the runner adds 3 framing stages
# (config load, success notify, ETL-health) for a total of [1/8]..[8/8].
STEP_COUNT = 5


def run(config, secrets, ctx):
    email_cfg = config["email"]
    midnight = datetime.combine(date.today(), time.min)

    # ── 1. Today's last successful load, if any ──
    # A mail that lands while that run is starting may be loaded twice; the
    # upsert makes that harmless.
    loaded_at = last_etl_success_since(config, midnight)
    if loaded_at is None:
        ctx.progress.step("No successful load yet today")
    else:
        ctx.progress.step(f"Loaded today at {loaded_at:%H:%M} — looking for a newer report")

    # ── 2. Download the newest Excel attachment since then ──
    try:
        save_path, latest_file = get_latest_excel_attachment(
            keyword=email_cfg["keyword"],
            destination_path=email_cfg["destination_path"],
            config=config,
            secrets=secrets,
            received_since=(loaded_at or midnight).astimezone(),
        )
    except FileNotFoundError:
        if loaded_at is not None:
            ctx.skip_reason = f"no newer report since today's load at {loaded_at:%H:%M}."
            return None
        expected_by = email_cfg.get("expected_by")
        if expected_by and datetime.now().time() < time.fromisoformat(expected_by):
            ctx.skip_reason = f"today's report has not arrived yet (expected by {expected_by})."
            return None
        raise
    ctx.source_file_path = save_path
    ctx.progress.step(f"Email attachment downloaded: {latest_file}")

    # ── 3. Ingest, unpivot & dedupe the key (a missing PRIMARY ML # raises -> FAIL) ──
    df = ingestion.prepare(save_path)
    ctx.row_count = len(df)
    report_date = ", ".join(str(d) for d in df["Report Date"].unique())
    ctx.progress.step(f"File ingested & unpivoted — {len(df)} rows, Report Date {report_date}")

    # ── 4. Stage, then upsert into the daily Autosub table ──
    counts = persistence.stage_and_upsert(df, config["persistence"])
    ctx.progress.step(
        f"Daily table upserted — {counts['inserted']} inserted, {counts['updated']} updated, "
        f"{counts['unchanged']} unchanged"
    )

    # ── 5. Upsert the (PRIMARY ML #, AutoSub) relations ──
    rel_counts = persistence.upsert_relations(df, config["persistence"])
    ctx.progress.step(
        f"Relations upserted — {rel_counts['inserted']} new, {rel_counts['updated']} changed, "
        f"{rel_counts['unchanged']} unchanged"
    )
    return save_path
