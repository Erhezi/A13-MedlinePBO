"""Load the substitute collection from PBO files already on disk.

Replays each raw PBO file, oldest first, through the same extract -> staging ->
MERGE path as the Tue/Thu run, so the staging table is left holding the newest
file's subs and re-running over files already loaded is harmless.

Usage (from the project root):
    python -m reports.medline_pbo.backfill_subs                       # Raw folder, files from 2026-08-13 on
    python -m reports.medline_pbo.backfill_subs <folder or files> --since 2026-08-13
    python -m reports.medline_pbo.backfill_subs --dry-run             # parse only, no DB

No email or ETL-health row is written; output goes to the console only.
"""

import argparse
import glob
import os
import re

from src.config_loader import load_config

from reports.medline_pbo import persistence
from reports.medline_pbo.ingestion import read_pbo_file

DEFAULT_CONFIG = os.path.join("reports", "medline_pbo", "config.yaml")
DEFAULT_SINCE = "2026-08-13"  # first automated export carrying the sub columns
# Raw names end in -yyyy-mm-dd-hh-mm-ss.xlsx under one of two prefixes
# ("... Risk Levels-" and "... Risk Levels Automated-"), so order by the stamp.
_STAMP = re.compile(r"(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\.xlsx$", re.IGNORECASE)


def file_stamp(path):
    match = _STAMP.search(os.path.basename(path))
    if not match:
        raise ValueError(f"Cannot read a yyyy-mm-dd-hh-mm-ss stamp from: {path}")
    return match.group(1)


def collect_files(paths, pattern, since):
    files = []
    for path in paths:
        if os.path.isdir(path):
            files.extend(glob.glob(os.path.join(path, pattern)))
        elif os.path.isfile(path):
            files.append(path)
        else:
            raise FileNotFoundError(f"Not a file or folder: {path}")
    files = [f for f in files if not os.path.basename(f).startswith("~$")]
    return sorted((f for f in files if file_stamp(f)[:10] >= since), key=file_stamp)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="*", help="Folders and/or .xlsx files (default: the Raw folder).")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="Earliest file date, yyyy-mm-dd.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only.")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    paths = args.paths or [config["email"]["destination_path"]]
    pattern = config["email"].get("attachment_prefix", "") + "*.xlsx"
    files = collect_files(paths, pattern, args.since)
    if not files:
        raise SystemExit(f"No files matching '{pattern}' dated {args.since} or later under {paths}.")

    # Parse every file first so a bad one stops the backfill before any write.
    required = config["report"]["required_columns"]
    prepared = [
        persistence.extract_sub_collection(read_pbo_file(path, required_columns=required), path)
        for path in files
    ]
    print(f"Prepared {sum(len(s) for s in prepared)} rows from {len(files)} file(s).")
    if args.dry_run:
        print("Dry run — nothing written to the database.")
        return

    for subs in prepared:
        persistence.stage_and_upsert(subs, config["persistence"])


if __name__ == "__main__":
    main()
