"""Monthly maintenance — clean old logs and archive old output files."""

import glob
import os
import shutil
from datetime import datetime, date


def should_run_maintenance(config):
    """Return True if today's day-of-month falls within the configured range."""
    lo, hi = config["maintenance"]["cleanup_day_range"]
    return lo <= date.today().day <= hi


def _first_of_month():
    """Return a datetime representing midnight on the 1st of the current month."""
    today = date.today()
    return datetime(today.year, today.month, 1).timestamp()


def cleanup_old_logs(config):
    """Delete log_*.txt files in the log directory that predate this month."""
    log_dir = config["logging"]["log_dir"]
    cutoff = _first_of_month()
    pattern = os.path.join(log_dir, "log_*.txt")
    removed = 0
    for path in glob.glob(pattern):
        if os.path.getmtime(path) < cutoff:
            os.remove(path)
            removed += 1
    print(f"Maintenance: removed {removed} old log file(s).")


def archive_old_outputs(config):
    """Move Processed_Monte_PBO*.xlsx older than this month to the archive."""
    output_dir = config["report"]["output_dir"]
    archive_dir = config["maintenance"]["output_archive_dir"]
    cutoff = _first_of_month()

    os.makedirs(archive_dir, exist_ok=True)
    pattern = os.path.join(output_dir, "Processed_Monte_PBO*.xlsx")
    moved = 0
    for path in glob.glob(pattern):
        if os.path.getmtime(path) < cutoff:
            dest = os.path.join(archive_dir, os.path.basename(path))
            shutil.move(path, dest)
            moved += 1
    print(f"Maintenance: archived {moved} old output file(s).")


def run_maintenance(config):
    """Run all maintenance tasks (logs cleanup + output archiving)."""
    if not should_run_maintenance(config):
        return
    print("Running monthly maintenance ...")
    try:
        cleanup_old_logs(config)
    except Exception as exc:
        print(f"Maintenance warning (logs cleanup): {exc}")
    try:
        archive_old_outputs(config)
    except Exception as exc:
        print(f"Maintenance warning (output archiving): {exc}")
