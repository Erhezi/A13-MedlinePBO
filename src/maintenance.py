"""Monthly maintenance — clean old logs and archive old output files."""

import glob
import os
import re
import shutil
from datetime import datetime, date

# "<yyyy>-<mm>-<dd>" anywhere in a file name, e.g. Medline's
# "WPH - FILL KILL REPORT-2026-09-20-01-02-06.xlsx".
_NAME_DATE = re.compile(r"(\d{4})-(0[1-9]|1[0-2])-\d{2}")


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


def _archive_month_folder(path):
    """Return "<yyyy>\\<mm>" for *path*: the date in its name, else its modified time.

    The name wins because a raw file's modified time is its latest download, and
    the newest mail is re-downloaded every run until a newer one arrives (a day's
    last file is saved again the next morning).
    """
    match = _NAME_DATE.search(os.path.basename(path))
    if match:
        year, month = match.groups()
    else:
        modified = date.fromtimestamp(os.path.getmtime(path))
        year, month = f"{modified.year:04d}", f"{modified.month:02d}"
    return os.path.join(year, month)


def archive_old_outputs(config):
    """Move output files older than this month to the archive.

    The filename glob is report-specific (``maintenance.output_glob``). With
    ``maintenance.archive_by_month: true`` each file goes to a
    ``<archive>\\<yyyy>\\<mm>`` subfolder (see ``_archive_month_folder``);
    otherwise straight into the archive folder.
    """
    output_dir = config["report"]["output_dir"]
    archive_dir = config["maintenance"]["output_archive_dir"]
    output_glob = config["maintenance"].get("output_glob", "Processed_Monte_PBO*.xlsx")
    by_month = config["maintenance"].get("archive_by_month", False)
    cutoff = _first_of_month()

    os.makedirs(archive_dir, exist_ok=True)
    pattern = os.path.join(output_dir, output_glob)
    moved = 0
    for path in glob.glob(pattern):
        if os.path.getmtime(path) < cutoff:
            dest_dir = archive_dir
            if by_month:
                dest_dir = os.path.join(archive_dir, _archive_month_folder(path))
                os.makedirs(dest_dir, exist_ok=True)
            shutil.move(path, os.path.join(dest_dir, os.path.basename(path)))
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
