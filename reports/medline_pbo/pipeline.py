"""Medline PBO report pipeline — report-specific steps.

Also persists the file's suggested substitutes to the MedlineSubCollection table
(see persistence.py) right after ingestion, so a database problem fails the
run before the slower transform.

Since v1.12 it also reads Medline's 3PL Item Inventory export (mailed Tue/Thu)
for the 3PL columns. That file is optional: when none is found the run warns
(log, ETL-health Error column, success email) and leaves the columns blank.

Returns the saved output path. The shared runner (src/runner.py) wraps this with
logging, success/failure notification, ETL-health logging, and maintenance.
"""

from src.db import get_connection, fetch_tables
from src.excel import reorder_columns, build_output_filename, apply_inventory_styling
from src.msgraph import get_latest_excel_attachment

from reports.medline_pbo import persistence
from reports.medline_pbo.queries import PBO_TEMPLATES
from reports.medline_pbo.ingestion import (
    read_pbo_file,
    validate_columns,
    apply_jesse_selection,
    extract_uom_table,
    read_3pl_file,
)
from reports.medline_pbo.transform import (
    prepare_location_inventory_tables,
    build_ehc,
    build_substitutes,
    merge_inventory,
    calculate_dioh_metrics,
    merge_substitutes,
    aggregate_item_groups,
    build_full_dataset,
    calculate_review_recommendations,
    build_uom_conversions,
    apply_uom_alternatives,
    assemble_output,
    merge_autosub,
    summarize_three_pl,
    merge_three_pl,
    add_desired_dioh_columns,
)

# Stages this pipeline reports; the runner adds 3 framing stages
# (config load, success notify, ETL-health) for a total of [1/10]..[10/10].
STEP_COUNT = 7


def _three_pl_mail_config(config):
    """Return *config* with ``email`` attachment rules swapped for the 3PL file's."""
    tpl = config["three_pl"]
    email = {
        **config["email"],
        "expected_sender": tpl["expected_sender"],
        "attachment_prefix": tpl["attachment_prefix"],
        "attachment_contains": [],
        "attachment_exclude": [],
        "attachment_extensions": tpl["attachment_extensions"],
        "inbox_lookback_days": tpl["inbox_lookback_days"],
    }
    return {**config, "email": email}


def _load_three_pl(config, secrets, ctx):
    """Download and summarize the newest 3PL Item Inventory file.

    Returns ``(summary, report_date)``, or ``(None, None)`` when no file is in
    the lookback window. A missing file is a warning, not an error. Each Order
    UM outside the known set is logged as an ETL-health warning.
    """
    tpl = config["three_pl"]
    try:
        save_path, file_name = get_latest_excel_attachment(
            keyword=tpl["keyword"],
            destination_path=tpl["destination_path"],
            config=_three_pl_mail_config(config),
            secrets=secrets,
        )
    except FileNotFoundError:
        days = tpl["inbox_lookback_days"]
        print(f"WARNING: no 3PL file in the last {days} days — 3PL columns left blank.")
        ctx.warnings.append(f"Warning: no 3PL Item Inventory file found in the last {days} days")
        ctx.email_notes.append(
            f"3PL file used: none found in the last {days} days (3PL columns are blank)"
        )
        ctx.progress.step("3PL file not found — 3PL columns left blank")
        return None, None

    df_3pl, report_date = read_3pl_file(save_path)
    summary, unknown_uoms = summarize_three_pl(df_3pl, tpl["uom_map"])
    for uom in unknown_uoms:
        print(f"WARNING: unexpected Order UM '{uom}' on the 3PL file; joined untranslated.")
        ctx.warnings.append(f"Warning: UOM '{uom}' seen on 3PL file")
    ctx.email_notes.append(f"3PL file used: {file_name}")
    ctx.progress.step(
        f"3PL file read: {file_name} — report date {report_date:%Y-%m-%d}, "
        f"{len(df_3pl)} rows, {len(summary)} Manufacturer Item + UOM keys"
    )
    return summary, report_date


def run(config, secrets, ctx):
    # ── 1. Download latest Excel attachment ──
    email_cfg = config["email"]
    save_path, latest_file = get_latest_excel_attachment(
        keyword=email_cfg["keyword"],
        destination_path=email_cfg["destination_path"],
        config=config,
        secrets=secrets,
    )
    if save_path is None:
        raise FileNotFoundError("No attachment found. Aborting.")
    ctx.source_file_path = save_path
    ctx.progress.step(f"Email attachment downloaded: {latest_file}")

    # ── 2. Ingest & validate ──
    report_cfg = config["report"]
    df = read_pbo_file(save_path, required_columns=report_cfg["required_columns"])
    validate_columns(df, report_cfg["required_columns"])
    df = apply_jesse_selection(
        df,
        risk_levels=report_cfg["risk_levels"],
        duration_threshold=report_cfg["duration_threshold"],
    )
    uom_df = extract_uom_table(df)
    ctx.row_count = len(df)
    ctx.progress.step(f"File ingested & validated — {ctx.row_count} rows")

    # ── 2b. 3PL Item Inventory (optional; warns when missing) ──
    three_pl_summary, three_pl_report_date = _load_three_pl(config, secrets, ctx)

    # ── 2c. Persist this file's suggested substitutes ──
    sub_counts = persistence.persist_file_subs(df, save_path, config["persistence"])
    ctx.progress.step(
        f"Substitutes persisted — {sub_counts['inserted']} new, "
        f"{sub_counts['updated']} updated, {sub_counts['unchanged']} unchanged"
    )

    # ── 3. Fetch database tables ──
    database_cfg = config["database"]
    locations = database_cfg.get("locations") or [database_cfg["location"]]
    conn = get_connection(config)
    tables = fetch_tables(conn, locations, PBO_TEMPLATES)
    conn.close()
    ctx.progress.step("Database tables fetched")

    # ── 4. Transform ──
    prepared_tables = prepare_location_inventory_tables(
        tables["inventory"], tables["usage"], tables["plmusage"],
    )
    df_ehc = build_ehc(
        prepared_tables["inventory"],
        prepared_tables["usage"],
        tables["long_desc"],
    )
    df_sub = build_substitutes(
        tables["plmlink"],
        prepared_tables["inventory"],
        prepared_tables["usage"],
        tables["long_desc"],
    )

    df_m = merge_inventory(df, df_ehc)
    df_m = calculate_dioh_metrics(df_m, report_cfg["review_threshold"])

    df_msub = merge_substitutes(df_m, df_sub)
    df_ig = aggregate_item_groups(
        df_msub, prepared_tables["plmusage"], report_cfg["review_threshold"],
    )
    df_full = build_full_dataset(df_msub, df_ig)

    df_review = calculate_review_recommendations(
        df_full, report_cfg["review_threshold"],
    )

    medline_cf_df = build_uom_conversions(uom_df)
    df_review_to_merge = apply_uom_alternatives(df_review, medline_cf_df)

    timestamp_value = tables["timestamp"].values[0][0]
    df_output = assemble_output(
        df_full,
        df_review_to_merge,
        timestamp_value,
        prepared_tables["ipyc_qty"],
    )
    df_output = merge_autosub(
        df_output, tables["autosub_relation"], tables["autosub_today"],
    )
    df_output = merge_three_pl(df_output, three_pl_summary, three_pl_report_date)
    df_output = add_desired_dioh_columns(
        df_output,
        target_date=report_cfg.get("desired_dioh_target_date", "2026-11-16"),
        overstock_date=report_cfg.get("overstock_check_date", "2026-11-09"),
    )
    ctx.progress.step(f"Data transformed — {len(df_output)} output rows")

    # ── 5. Export styled report ──
    df_output_reordered = reorder_columns(df_output, config)
    output_path = build_output_filename(latest_file, config)
    output_path = apply_inventory_styling(
        df_output_reordered,
        output_path,
        config,
        extra_sheets=[("UOM inconsistency", prepared_tables["uom_inconsistency"])],
    )
    ctx.progress.step(f"Report exported — {output_path}")
    return output_path
