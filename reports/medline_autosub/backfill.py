"""Load Daily Autosub exports already on disk, oldest first, one file at a time.

Used for the initial history load and for any day the scheduled run missed.
Each file goes through the same staging -> MERGE path as a daily run, so
re-running over files already loaded is harmless.

Usage (from the project root):
    python -m reports.medline_autosub.backfill <folder>
    python -m reports.medline_autosub.backfill <file1.xls> <file2.xls> ...
    python -m reports.medline_autosub.backfill <folder> --dry-run   # parse only, no DB

No email or ETL-health row is written, so the scheduled run still loads the
day's newest file; output goes to the console only.
"""

import argparse
import glob
import os

from src.config_loader import load_config

from reports.medline_autosub import ingestion, persistence

DEFAULT_CONFIG = os.path.join("reports", "medline_autosub", "config.yaml")


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
    parser.add_argument("paths", nargs="+", help="Folders and/or .xls files to load.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only.")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    pattern = config["email"].get("attachment_prefix", "") + "*.xls"
    files = collect_files(args.paths, pattern)
    if not files:
        raise SystemExit(f"No files matching '{pattern}' under {args.paths}.")

    # Validate every file up front so a key violation in any of them stops the
    # backfill before anything is written. File names end in a
    # yyyy-mm-dd-hh-mm-ss stamp under a fixed prefix, so a name sort is
    # chronological — a later file wins on AutoSub Seq / PRIMARY PARENT in the
    # relation table.
    files = sorted(files, key=os.path.basename)
    prepared = [ingestion.prepare(path) for path in files]
    print(f"Prepared {sum(len(df) for df in prepared)} rows from {len(files)} file(s).")
    if args.dry_run:
        print("Dry run — nothing written to the database.")
        return

    for df in prepared:
        persistence.persist_file(df, config["persistence"])


if __name__ == "__main__":
    main()
