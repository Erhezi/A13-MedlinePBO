"""Medline PBO pipeline — end-to-end entry point.

Usage:
    python main.py                      # uses config.yaml in cwd
    python main.py --config other.yaml  # custom config path
"""

import argparse
import os
import traceback
import warnings
from datetime import datetime

from src.config_loader import load_config, load_secrets
from src.db import get_connection, fetch_all_tables, insert_etl_health
from src.ingestion import (
    read_pbo_file,
    validate_columns,
    apply_jesse_selection,
    extract_uom_table,
)
from src.logger import TeeLogger
from src.maintenance import run_maintenance
from src.msgraph import (
    get_latest_excel_attachment,
    send_success_notification,
    send_failure_notification,
)
from src.transform import (
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
from src.report import (
    reorder_columns,
    build_output_filename,
    apply_inventory_styling,
)

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

PACKAGE_PATH = os.path.abspath(__file__)


def main(config_path="config.yaml"):
    # ── 0. Load config & secrets, start logger ──
    start_time = datetime.now()
    config = load_config(config_path)
    secrets = load_secrets()

    logger = TeeLogger(config["logging"]["log_dir"])

    source_file_path = ""
    row_count = 0

    try:
        print(f"Pipeline started at {start_time:%Y-%m-%d %H:%M:%S}")
        print("[1/8] Config & secrets loaded.")

        # ── 1. Download latest Excel attachment ──
        email_cfg = config["email"]
        save_path, latest_file = get_latest_excel_attachment(
            keyword=email_cfg["keyword"],
            destination_path=email_cfg["destination_path"],
            config=config,
            secrets=secrets,
        )
        if save_path is None:
            raise SystemExit("No attachment found. Aborting.")
        source_file_path = save_path
        print(f"[2/8] Excel downloaded: {latest_file}")

        # ── 2. Ingest & validate ──
        report_cfg = config["report"]
        df = read_pbo_file(save_path)
        validate_columns(df, report_cfg["required_columns"])
        df = apply_jesse_selection(
            df,
            risk_levels=report_cfg["risk_levels"],
            duration_threshold=report_cfg["duration_threshold"],
        )
        uom_df = extract_uom_table(df)
        row_count = len(df)
        print(f"[3/8] Ingestion complete — {row_count} rows.")

        # ── 3. Fetch database tables ──
        location = config["database"]["location"]
        conn = get_connection(config)
        tables = fetch_all_tables(conn, location)
        conn.close()
        print("[4/8] Database tables fetched.")

        # ── 4. Transform ──
        df_ehc = build_ehc(tables["inventory"], tables["usage"], tables["long_desc"])
        df_sub = build_substitutes(
            tables["plmlink"], tables["inventory"], tables["usage"], tables["long_desc"],
        )

        df_m = merge_inventory(df, df_ehc)
        df_m = calculate_dioh_metrics(df_m, report_cfg["review_threshold"])

        df_msub = merge_substitutes(df_m, df_sub)
        df_ig = aggregate_item_groups(
            df_msub, tables["plmusage"], report_cfg["review_threshold"],
        )
        df_full = build_full_dataset(df_msub, df_ig)

        df_review = calculate_review_recommendations(
            df_full, report_cfg["review_threshold"],
        )

        medline_cf_df = build_uom_conversions(uom_df)
        df_review_to_merge = apply_uom_alternatives(df_review, medline_cf_df)

        timestamp_value = tables["timestamp"].values[0][0]
        df_output = assemble_output(df_full, df_review_to_merge, timestamp_value)
        print(f"[5/8] Transformation complete — {len(df_output)} output rows.")

        # ── 5. Export styled report ──
        df_output_reordered = reorder_columns(df_output, config)
        output_path = build_output_filename(latest_file, config)
        apply_inventory_styling(df_output_reordered, output_path, config)
        print(f"[6/8] Report saved — {output_path}")

        # ── 6. Send success email ──
        send_success_notification(config, secrets, output_path)
        print("[7/8] Success notification sent.")

        # ── 7. Log ETL health (SUCCESS) ──
        insert_etl_health(
            config,
            source_file_path=source_file_path,
            last_run_time=start_time,
            task_status="SUCCESS",
            row_count=row_count,
            package_path=PACKAGE_PATH,
            log_file_path=logger.log_path,
            error_message="",
        )
        print("[8/8] ETL health logged.")

    except Exception:
        traceback.print_exc()  # captured by TeeLogger
        # close logger early so the log file is complete for the attachment
        log_path = logger.close()
        logger = None

        try:
            send_failure_notification(config, secrets, log_path)
        except Exception as mail_exc:
            print(f"Failed to send failure notification: {mail_exc}")

        try:
            insert_etl_health(
                config,
                source_file_path=source_file_path,
                last_run_time=start_time,
                task_status="FAIL",
                row_count=row_count,
                package_path=PACKAGE_PATH,
                log_file_path=log_path,
                error_message="See Log",
            )
        except Exception as db_exc:
            print(f"Failed to insert ETL health: {db_exc}")

    finally:
        if logger is not None:
            logger.close()

        try:
            run_maintenance(config)
        except Exception as mnt_exc:
            print(f"Maintenance error (non-fatal): {mnt_exc}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medline PBO report pipeline")
    parser.add_argument(
        "--config", default="config.yaml", help="Path to YAML config file",
    )
    args = parser.parse_args()
    main(args.config)
