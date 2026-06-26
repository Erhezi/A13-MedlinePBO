"""Medline Allocation report pipeline — report-specific steps.

Stateful: upserts this week's rows into the WeeklyAlloc* helper tables, then
reads prior weeks back to compute risk-of-loss (so the upsert MUST precede the
history read). Returns the saved output path; the shared runner (src/runner.py)
provides logging, notification, ETL-health, and maintenance.
"""

import pandas as pd

from src.db import get_connection, get_pyodbc_connection, get_engine, fetch_tables
from src.excel import reorder_columns, build_output_filename, apply_inventory_styling
from src.msgraph import get_latest_excel_attachment

from reports.medline_allocation import ingestion, persistence, transform
from reports.medline_allocation.queries import ALLOCATION_TEMPLATES, LOST_ALLOC_SQL
from reports.medline_allocation.weeks import get_prior_x_week_yearweek


def run(config, secrets, ctx):
    email_cfg = config["email"]
    report_cfg = config["report"]

    # ── 1. Download latest Excel attachment ──
    save_path, latest_file = get_latest_excel_attachment(
        keyword=email_cfg["keyword"],
        destination_path=email_cfg["destination_path"],
        config=config,
        secrets=secrets,
    )
    if save_path is None:
        raise FileNotFoundError("No attachment found. Aborting.")
    ctx.source_file_path = save_path
    print(f"Excel downloaded: {latest_file}")

    # ── 2. Ingest & cleanse ──
    df = ingestion.read_allocation_file(save_path)
    ingestion.validate_columns(df, report_cfg["required_columns"])
    df = ingestion.remove_footer(df)
    df = ingestion.apply_jesse_selection(df, c_group=report_cfg["c_group"])
    df = ingestion.make_year_week(df)
    df = ingestion.clean_int_cols(df, report_cfg["int_cols"])
    ctx.row_count = len(df)

    df["Dummy ID"] = [(i + 1) for i in range(1, len(df) + 1)]
    df_small = df[df["C Group"] == report_cfg["c_group"]].copy()
    df_mini = df[df["Jesse Selection"] == "x"].copy()
    print(f"Ingestion complete — {len(df)} rows ({len(df_mini)} selected).")

    # ── 3. Persist this week, THEN read prior weeks (order matters) ──
    persist_cfg = config["persistence"]
    engine = get_engine(persist_cfg)
    current_yearweek = persistence.upsert_tracking(
        df_mini, engine, persist_cfg["schema"], persist_cfg["tracking_table"],
    )
    persistence.upsert_unspsc(
        df, engine, persist_cfg["schema"], persist_cfg["unspsc_table"],
    )

    prior_weeks = [get_prior_x_week_yearweek(n) for n in (1, 2, 3, 4)]
    min_yearweek = get_prior_x_week_yearweek(report_cfg["lost_alloc_lookback_weeks"])

    # ── 4. Fetch database tables ──
    locations = config["database"]["locations"]
    conn = get_connection(config)
    tables = fetch_tables(conn, locations, ALLOCATION_TEMPLATES)
    conn.close()

    lost_alloc_conn = get_pyodbc_connection(persist_cfg)
    df_lost_alloc = pd.read_sql_query(
        LOST_ALLOC_SQL.format(
            schema=persist_cfg["schema"],
            table=persist_cfg["tracking_table"],
            min_yearweek=min_yearweek,
        ),
        lost_alloc_conn,
    )
    lost_alloc_conn.close()
    print("Database tables fetched.")

    # ── 5. Transform ──
    df_inv_agg, df_usage_agg, df_ehc = transform.build_enrichment(
        tables["inventory"], tables["usage"], tables["long_desc"],
    )
    df_sub = transform.build_substitutes(
        tables["plmlink"], df_inv_agg, df_usage_agg, tables["long_desc"],
    )
    df_mini_itemlabel = transform.resolve_item_and_ea(df_mini, tables["collect_item"])
    df_lost_alloc_to_join = transform.build_risk_of_loss(
        df_lost_alloc, current_yearweek, prior_weeks,
    )
    df_m = transform.merge_and_metrics(df_mini_itemlabel, df_lost_alloc_to_join, df_ehc)
    df_msub = transform.merge_substitutes(df_m, df_sub)
    df_ig = transform.aggregate_item_groups(df_msub, tables["plmusage"])
    df_full = transform.build_full(df_msub, df_ig)
    df_full, df_review = transform.recommend(df_full, report_cfg["target_dioh"])

    timestamp_value = tables["timestamp"].values[0][0]
    df_output_all = transform.assemble_output(
        df_full, df_review, df_small, current_yearweek, timestamp_value,
    )
    print(f"Transformation complete — {len(df_output_all)} output rows.")

    # ── 6. Export styled report (only the current YearWeek stays visible) ──
    df_reordered = reorder_columns(df_output_all, config)
    output_path = build_output_filename(latest_file, config)
    output_path = apply_inventory_styling(
        df_reordered, output_path, config, filter_value=current_yearweek,
    )
    print(f"Report saved — {output_path}")
    return output_path
