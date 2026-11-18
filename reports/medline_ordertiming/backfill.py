"""Load Order Detail exports already on disk: the fullest file of each Report Date.

Used for the initial history load and for re-loading a day the scheduled run
missed. A load replaces its whole Report Date, so only one file per day
matters: the one with the most lines (the day's lines only grow), ties going to
the later file name. File-name stamps can't order a day's files on their own:
they look like a 12-hour clock (Central time), so 01-07-19 may be 1 AM or 1 PM.

Usage (from the project root):
    python -m reports.medline_ordertiming.backfill <folder>
    python -m reports.medline_ordertiming.backfill <file1.xls> <file2.xls> ...
    python -m reports.medline_ordertiming.backfill <folder> --dry-run   # parse only, no DB

No email or ETL-health row is written; output goes to the console only.
"""

import argparse
import glob
import os

from src.config_loader import load_config

from reports.medline_ordertiming import ingestion, persistence

DEFAULT_CONFIG = os.path.join("reports", "medline_ordertiming", "config.yaml")


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

    # Validate every file up front so a bad file stops the backfill before
    # anything is written, then keep the fullest file of each Report Date.
    best = {}
    for path in sorted(files, key=os.path.basename):
        prepared = ingestion.prepare(path)
        report_date = prepared.report_date
        if report_date not in best or len(prepared.rows) >= len(best[report_date][1].rows):
            best[report_date] = (path, prepared)

    print(f"\n{len(files)} file(s) -> {len(best)} Report Date(s):")
    for report_date in sorted(best):
        path, prepared = best[report_date]
        print(f"  {report_date}: {len(prepared.rows)} line(s), {len(prepared.lots)} lot(s) "
              f"from {os.path.basename(path)}")
    if args.dry_run:
        print("Dry run — nothing written to the database.")
        return

    for report_date in sorted(best):
        prepared = best[report_date][1]
        persistence.replace_day(prepared.rows, report_date, config["persistence"])
        persistence.upsert_lots(prepared.lots, config["persistence"])


if __name__ == "__main__":
    main()
