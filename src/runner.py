"""Shared run harness for every report.

Each report ships a thin entry script (``run_<report>.py``) that calls
``runner.run(__file__, pipeline_fn)``. The runner provides everything common:
the timeout watchdog (parent re-invokes itself as a ``--worker`` subprocess),
logging, success/failure notification, ETL-health logging, and maintenance.

A pipeline function has the signature ``pipeline_fn(config, secrets, ctx)`` and
returns the saved output path. It updates ``ctx.source_file_path`` and
``ctx.row_count`` as it progresses so the runner can log ETL health on both the
success and failure paths.
"""

import os
import subprocess
import sys
import traceback
from datetime import datetime

from src.config_loader import load_config, load_secrets, resolve_config_path
from src.db import insert_etl_health
from src.logger import TeeLogger
from src.maintenance import run_maintenance
from src.msgraph import send_success_notification, send_failure_notification
from src.secret_crypto import SECRET_ENV_VAR

PIPELINE_TIMEOUT_SECONDS = 15 * 60


class RunContext:
    """Mutable per-run state the pipeline updates for ETL-health logging."""

    def __init__(self):
        self.source_file_path = ""
        self.row_count = 0


def _load_runtime_config_and_secrets(config_path):
    resolved_config_path = resolve_config_path(config_path)
    config = load_config(resolved_config_path)
    try:
        secrets = load_secrets()
    except RuntimeError as exc:
        if SECRET_ENV_VAR in str(exc):
            raise SystemExit(
                "Missing encrypted-secret passphrase. Run 'python first_time_setup.py' first."
            ) from exc
        raise
    return config, secrets, resolved_config_path


def _build_log_path(log_dir, start_time):
    stamp = start_time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"log_{stamp}.txt")


def _append_timeout_to_log(log_path, start_time):
    timeout_minutes = PIPELINE_TIMEOUT_SECONDS // 60
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write("\n")
        log_file.write(
            f"TIMEOUT: Pipeline exceeded {timeout_minutes} minutes and was terminated at "
            f"{datetime.now():%Y-%m-%d %H:%M:%S}.\n"
        )
        log_file.write(f"Pipeline start time was {start_time:%Y-%m-%d %H:%M:%S}.\n")


def _elapsed_seconds(start_time, end_time=None):
    if end_time is None:
        end_time = datetime.now()
    return int((end_time - start_time).total_seconds())


def _handle_timeout_failure(config, secrets, start_time, log_path, package_path):
    duration = _elapsed_seconds(start_time)
    try:
        send_failure_notification(config, secrets, log_path)
    except Exception as mail_exc:
        print(f"Failed to send failure notification: {mail_exc}")
    try:
        insert_etl_health(
            config,
            source_file_path="",
            last_run_time=start_time,
            task_status="TIMEOUT",
            row_count=0,
            duration=duration,
            package_path=package_path,
            log_file_path=log_path,
            error_message=f"Pipeline exceeded {PIPELINE_TIMEOUT_SECONDS // 60} minutes",
        )
    except Exception as db_exc:
        print(f"Failed to insert ETL health: {db_exc}")


def run_worker(pipeline_fn, config_path, log_path, package_path):
    """Worker process: run the pipeline and log/notify around it."""
    start_time = datetime.now()
    config, secrets, _ = _load_runtime_config_and_secrets(config_path)
    logger = TeeLogger(config["logging"]["log_dir"], log_path=log_path)
    ctx = RunContext()

    try:
        print(f"Pipeline started at {start_time:%Y-%m-%d %H:%M:%S}")
        print("Config & secrets loaded.")

        output_path = pipeline_fn(config, secrets, ctx)

        send_success_notification(config, secrets, output_path)
        print("Success notification sent.")

        insert_etl_health(
            config,
            source_file_path=ctx.source_file_path,
            last_run_time=start_time,
            task_status="SUCCESS",
            row_count=ctx.row_count,
            duration=_elapsed_seconds(start_time),
            package_path=package_path,
            log_file_path=logger.log_path,
            error_message="",
        )
        print("ETL health logged.")

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
                source_file_path=ctx.source_file_path,
                last_run_time=start_time,
                task_status="FAIL",
                row_count=ctx.row_count,
                duration=_elapsed_seconds(start_time),
                package_path=package_path,
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


def run_parent(entry_path, config_path, package_path, forward_args=()):
    """Parent process: launch the worker as a subprocess with a hard timeout.

    *forward_args* are inserted into the worker command line (e.g.
    ``["--report", "medline_pbo"]``) so the child re-selects the same report.
    """
    start_time = datetime.now()
    config, secrets, resolved_config_path = _load_runtime_config_and_secrets(config_path)
    log_path = _build_log_path(config["logging"]["log_dir"], start_time)
    cmd = [
        sys.executable,
        entry_path,
        *forward_args,
        "--config",
        resolved_config_path,
        "--worker",
        "--log-path",
        log_path,
    ]

    try:
        completed = subprocess.run(cmd, timeout=PIPELINE_TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired:
        _append_timeout_to_log(log_path, start_time)
        print(
            f"TIMEOUT: Pipeline exceeded {PIPELINE_TIMEOUT_SECONDS // 60} minutes and was terminated."
        )
        _handle_timeout_failure(config, secrets, start_time, log_path, package_path)
        raise SystemExit(1)

    raise SystemExit(completed.returncode)
