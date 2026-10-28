"""Load Fill-Kill exports already on disk, oldest first, one file at a time.

Used for the initial history load and for re-loading any hourly file the
scheduled run missed. Each file goes through the same staging -> MERGE path as
an hourly run, so the staging table is left holding only the newest file, and
re-running over files already loaded is harmless.

Usage (from the project root):
    python -m reports.medline_fillkill.backfill "I:\\Procurement PMO\\dli2\\Medline FillKill\\test"
    python -m reports.medline_fillkill.backfill <file1.xlsx> <file2.xlsx> ...
    python -m reports.medline_fillkill.backfill <folder> --dry-run   # parse only, no DB

No email or ETL-health row is written; output goes to the console only.
"""

import argparse
import glob
import os

from src.config_loader import load_config

from reports.medline_fillkill import ingestion, persistence

DEFAULT_CONFIG = os.path.join("reports", "medline_fillkill", "config.yaml")


def collect_files(paths, pattern):
    files = []
    for path in paths:
        if os.path.isdir(path):
            files.extend(glob.glob(os.path.join(path, pattern)))
        elif os.path.isfile(path):
            files.append(path)
        else:
            raise FileNotFoundError(f"Not a file or folder: {path}")
    # Excel lock files ("~$...") are not exports.
    return [f for f in files if not os.path.basename(f).startswith("~$")]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="+", help="Folders and/or .xlsx files to load.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only.")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    pattern = config["email"].get("attachment_prefix", "") + "*.xlsx"
    files = collect_files(args.paths, pattern)
    if not files:
        raise SystemExit(f"No files matching '{pattern}' under {args.paths}.")

    # Validate every file up front so a primary-key violation in any of them
    # stops the backfill before anything is written. File names end in a
    # yyyy-mm-dd-hh-mm-ss stamp under a fixed prefix, so a name sort is
    # chronological.
    files = sorted(files, key=os.path.basename)
    prepared = [ingestion.prepare(path) for path in files]
    print(f"Prepared {sum(len(df) for df in prepared)} rows from {len(files)} file(s).")
    if args.dry_run:
        print("Dry run — nothing written to the database.")
        return

    # Replay the files one at a time, oldest first, exactly as the hourly runs
    # would have: the staging table ends up holding only the newest file, and a
    # later file wins on an order line that appears in more than one file.
    for df in prepared:
        persistence.stage_and_upsert(df, config["persistence"])


if __name__ == "__main__":
    main()
