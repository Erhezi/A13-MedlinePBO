"""Medline PBO report pipeline — report-specific steps.

Returns the saved output path. The shared runner (src/runner.py) wraps this with
logging, success/failure notification, ETL-health logging, and maintenance.
"""

from src.db import get_connection, fetch_tables
from src.excel import reorder_columns, build_output_filename, apply_inventory_styling
from src.msgraph import get_latest_excel_attachment

from reports.medline_pbo.ingestion import (
    read_pbo_file,
    validate_columns,
    apply_jesse_selection,
    extract_uom_table,
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
)

# Stages this pipeline reports; the runner adds 3 framing stages
# (config load, success notify, ETL-health) for a total of [1/8]..[8/8].
STEP_COUNT = 5


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

    # ── 3. Fetch database tables ──
    database_cfg = config["database"]
    locations = database_cfg.get("locations") or [database_cfg["location"]]
    conn = get_connection(config)
    tables = fetch_tables(conn, locations)
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
        prepared_tables["ipyc_items"],
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
