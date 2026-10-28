"""Ingestion & reshaping for Medline's Daily Autosub Management report.

The source is Medline's "Daily Autosub Management Report-<yyyy-mm-dd-hh-mm-ss>.xls"
export: one header row, one row per primary item, then a footer — a blank row,
the report title, and "Report Created: mm/dd/yyyy" — sitting in the first column
with every other cell empty. The footer rows are dropped by shape and the
"Report Created" date becomes ``Report Date``.

Each row carries up to four auto-substitutes side by side: AS1..AS4, each with
its own Branch Forecast / DED QTY / MIOH (plus END DATE, sub impact and NEXT ETA,
which are not loaded). ``unpivot`` turns them into one row per
(PRIMARY ML #, AutoSub) with ``AutoSub Seq`` 1-4; an empty slot (blank, or the
"0" Medline puts in unused AS2-AS4 cells) yields no row.

A row with no PRIMARY ML # fails the run before anything is written. A repeated
(PRIMARY ML #, AutoSub, Report Date) keeps its first row in file order (row by
row, AS1..AS4 within a row); the repeats are dropped with a NOTE in the log.

Column names follow the source except "ETA for Primary?", loaded as
"ETA for Primary", and the per-slot columns, which lose their slot suffix.
"""

import os
from datetime import datetime

import pandas as pd

KEY_COLUMNS = ["PRIMARY ML #", "AutoSub", "Report Date"]

# Source name -> loaded name for the per-primary columns.
PRIMARY_COLUMNS = {
    "Branch Number": "Branch Number",
    "Customer Group": "Customer Group",
    "Customer Number": "Customer Number",
    "PRIMARY PARENT": "PRIMARY PARENT",
    "PRIMARY ML #": "PRIMARY ML #",
    "FCST": "FCST",
    "MIOH1": "MIOH1",
    "ETA for Primary?": "ETA for Primary",
}

# AutoSub Seq -> the slot's source columns, in SLOT_COLUMNS order. Slot 1's
# columns carry no suffix; "MIOH1" is the primary's MIOH, not slot 1's.
SLOT_COLUMNS = ["AutoSub", "Branch Forecast", "DED QTY", "MIOH"]
AUTOSUB_SLOTS = {
    1: ["AS1", "Branch Forecast", "DED QTY", "MIOH"],
    2: ["AS2", "Branch Forecast2", "DED QTY2", "MIOH2"],
    3: ["AS3", "Branch Forecast3", "DED QTY3", "MIOH3"],
    4: ["AS4", "Branch Forecast4", "DED QTY4", "MIOH4"],
}

SOURCE_COLUMNS = list(PRIMARY_COLUMNS) + [c for cols in AUTOSUB_SLOTS.values() for c in cols]
SOURCE_FILE_COLUMN = "source file"

# Column order shared by the staging and target tables (see persistence.py).
LOAD_COLUMNS = (
    list(PRIMARY_COLUMNS.values())
    + ["AutoSub", "AutoSub Seq", "Branch Forecast", "DED QTY", "MIOH", "Report Date"]
    + [SOURCE_FILE_COLUMN]
)

INT_COLUMNS = ["FCST", "Branch Forecast", "DED QTY"]
DECIMAL_COLUMNS = ["MIOH1", "MIOH"]

REPORT_DATE_PATTERN = r"Report Created:\s*(\d{1,2}/\d{1,2}/\d{4})"


def read_autosub_file(file_path):
    """Read one export: strip names/values, validate, split off the footer.

    Returns (rows, report_date).
    """
    df = pd.read_excel(file_path, engine="calamine", dtype=str)
    df.columns = [str(col).strip() for col in df.columns]
    validate_columns(df, SOURCE_COLUMNS)
    for col in df.columns:
        df[col] = df[col].str.strip().replace("", pd.NA)
    df, footer = split_footer(df)
    report_date = parse_report_date(footer, os.path.basename(file_path))
    print(f"Read {len(df)} rows from {os.path.basename(file_path)} (Report Date {report_date}).")
    return df[SOURCE_COLUMNS].copy(), report_date


def validate_columns(df, required_columns):
    """Raise ValueError if any *required_columns* are missing from *df*."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Validation Failed. Missing columns: {missing}")
    print("Column validation successful.")
    return True


def split_footer(df):
    """Split off the footer: rows whose columns other than the first are all blank.

    No real row looks like that (every row has a PRIMARY ML #), so a data row
    with a missing key is kept and fails ``validate_keys`` loudly instead.
    """
    footer = df[df.columns[1:]].isna().all(axis=1)
    return df[~footer].reset_index(drop=True), df[footer]


def parse_report_date(footer, source_name):
    """Return the date from the footer's "Report Created: mm/dd/yyyy" line."""
    matches = footer.iloc[:, 0].dropna().str.extract(REPORT_DATE_PATTERN, expand=False).dropna()
    if matches.empty:
        raise ValueError(f"{source_name}: no 'Report Created: mm/dd/yyyy' line in the footer.")
    return datetime.strptime(matches.iloc[0], "%m/%d/%Y").date()


def unpivot(df, report_date, source_name):
    """One row per (primary, filled AutoSub slot), with AutoSub Seq 1-4, in file order."""
    primary = df[list(PRIMARY_COLUMNS)].rename(columns=PRIMARY_COLUMNS)
    slots = []
    for seq, cols in AUTOSUB_SLOTS.items():
        slot = df[cols].set_axis(SLOT_COLUMNS, axis=1)
        slots.append(pd.concat([primary, slot], axis=1).assign(**{"AutoSub Seq": seq}))
    # A stable sort on the source row index gives file order: row 1's AS1..AS4,
    # then row 2's, ... — what drop_duplicate_keys means by "first".
    long = pd.concat(slots).sort_index(kind="stable")
    long = long[long["AutoSub"].notna() & (long["AutoSub"] != "0")]

    no_sub = ~primary["PRIMARY ML #"].isin(long["PRIMARY ML #"]) & primary["PRIMARY ML #"].notna()
    if no_sub.any():
        print(
            f"NOTE: {no_sub.sum()} row(s) have no AutoSub in any slot and were skipped, "
            f"e.g. {primary.loc[no_sub, 'PRIMARY ML #'].head(5).tolist()}"
        )

    long["Report Date"] = report_date
    long[SOURCE_FILE_COLUMN] = os.path.basename(source_name)
    return long.reset_index(drop=True)


def coerce_types(df):
    """ETA -> date, forecasts/quantities -> int, MIOH -> 2 decimals.

    Parsing is strict so an unexpected format fails the run (and triggers the
    failure email) rather than silently loading blanks.
    """
    df["ETA for Primary"] = pd.to_datetime(df["ETA for Primary"], format="mixed").dt.date
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round().astype("Int64")
    for col in DECIMAL_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round(2)
    df["AutoSub Seq"] = df["AutoSub Seq"].astype("Int64")
    return df


def validate_keys(df, source_name):
    """Fail if any row is missing PRIMARY ML #.

    A violation raises, so the run fails before touching the database and the
    runner logs FAIL to ETL health and sends the failure email with this
    message in the log.
    """
    missing_key = df["PRIMARY ML #"].isna()
    if missing_key.any():
        raise ValueError(
            f"{source_name}: {missing_key.sum()} AutoSub row(s) have no 'PRIMARY ML #', "
            f"e.g. AutoSub {df.loc[missing_key, 'AutoSub'].head(5).tolist()}."
        )


def drop_duplicate_keys(df):
    """Keep the first row (in file order) of each repeated key; drop the rest.

    (PRIMARY ML #, AutoSub, Report Date) is the primary key of the staging table
    and the unique key of the target table. Medline occasionally lists a primary
    twice (2026-10-28: NON256025 on two rows differing only in DED QTY, 14 then
    7).
    """
    dup = df.duplicated(subset=KEY_COLUMNS, keep="first")
    if dup.any():
        dup_keys = df.loc[dup, KEY_COLUMNS].drop_duplicates()
        examples = [tuple(map(str, k)) for k in dup_keys.head(10).itertuples(index=False)]
        print(
            f"NOTE: {dup.sum()} row(s) repeat an earlier key and were dropped "
            f"(first row kept), e.g. {examples}"
        )
    df = df[~dup].reset_index(drop=True)
    print(f"Key check passed — {len(df)} unique {KEY_COLUMNS}.")
    return df


def prepare(file_path):
    """Read, unpivot, validate and cleanse ONE export into the load shape."""
    source_name = os.path.basename(file_path)
    df, report_date = read_autosub_file(file_path)
    df = unpivot(df, report_date, source_name)
    validate_keys(df, source_name)
    df = drop_duplicate_keys(df)
    df = coerce_types(df)
    df = df.sort_values(["PRIMARY ML #", "AutoSub Seq"], ignore_index=True)
    return df[LOAD_COLUMNS]
