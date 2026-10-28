"""Ingestion & cleansing for the Medline Fill-Kill feed.

The source is Medline's "WPH - FILL KILL REPORT-<yyyy-mm-dd-hh-mm-ss>.xlsx"
export: one header row, the order lines, then a 3-line footer whose first cell
carries the report title / date range / run date and whose other cells are
blank. Those footer rows are dropped by shape (see ``remove_footer``).

Every export must honour the primary key (Sales Order #, Order Line #): a
missing or repeated key fails the run before anything is written.

Source column names are kept as-is (whitespace-stripped). Two derived columns
are appended:

- ``PO``     — the leading 10 digits of ``PO #`` ("2600301531-STPO" -> "2600301531")
- ``POLine`` — ``PO Line`` with leading zeros removed ("000003" -> 3)
"""

import os

import pandas as pd

KEY_COLUMNS = ["Sales Order #", "Order Line #"]

SOURCE_COLUMNS = [
    "Sold-To", "Sold-To Name", "Ship-To", "Ship-To Name", "Ship-To Name 2",
    "Order Date", "Order Time", "PO #", "PO Line", "Sales Order #",
    "Order Line #", "Item", "Item Description", "Item # on PO",
    "Customer Item #", "Mfr. Item #", "Manufacturer Name", "Order Qty", "UM",
    "Filled Qty", "Plant", "Buyer Name",
    "Internal Order Comments from SAP (Z106)",
]
DERIVED_COLUMNS = ["PO", "POLine"]
SOURCE_FILE_COLUMN = "source file"

# Column order shared by the staging and target tables (see persistence.py).
LOAD_COLUMNS = SOURCE_COLUMNS + DERIVED_COLUMNS + [SOURCE_FILE_COLUMN]

INT_COLUMNS = ["Order Qty", "Filled Qty"]


def read_fillkill_file(file_path):
    """Read one export: strip names/values, validate, trim the footer, tag the file."""
    df = pd.read_excel(file_path, engine="calamine", dtype=str)
    df.columns = [str(col).strip() for col in df.columns]
    validate_columns(df, SOURCE_COLUMNS)
    df = df[SOURCE_COLUMNS].copy()
    for col in SOURCE_COLUMNS:
        df[col] = df[col].str.strip().replace("", pd.NA)
    df = remove_footer(df)
    df[SOURCE_FILE_COLUMN] = os.path.basename(file_path)
    print(f"Read {len(df)} rows from {os.path.basename(file_path)}")
    return df


def validate_columns(df, required_columns):
    """Raise ValueError if any *required_columns* are missing from *df*."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Validation Failed. Missing columns: {missing}")
    print("Column validation successful.")
    return True


def remove_footer(df):
    """Drop the footer: rows whose columns other than the first are all blank.

    The footer's report title / date range / run date sit in the first column
    with every other cell empty, which no real order line looks like. Matching
    on that shape (rather than cutting at the first blank key) means an order
    line with a missing key is kept and fails ``validate_keys`` loudly instead
    of silently truncating the file.
    """
    footer = df[SOURCE_COLUMNS[1:]].isna().all(axis=1)
    if footer.any():
        print(f"Footer removed ({footer.sum()} row(s)).")
    return df[~footer].reset_index(drop=True).copy()


def add_po_columns(df):
    """Append PO (leading 10 digits of 'PO #') and POLine ('PO Line' sans zeros)."""
    df["PO"] = df["PO #"].str.extract(r"^(\d{10})", expand=False)
    df["POLine"] = pd.to_numeric(df["PO Line"], errors="coerce").astype("Int64")

    bad_po = df["PO"].isna() & df["PO #"].notna()
    if bad_po.any():
        print(
            f"WARNING: {bad_po.sum()} row(s) have a 'PO #' that does not start with "
            f"10 digits; PO left blank. e.g. {df.loc[bad_po, 'PO #'].head(3).tolist()}"
        )
    bad_line = df["POLine"].isna() & df["PO Line"].notna()
    if bad_line.any():
        print(
            f"WARNING: {bad_line.sum()} row(s) have a non-numeric 'PO Line'; POLine "
            f"left blank. e.g. {df.loc[bad_line, 'PO Line'].head(3).tolist()}"
        )
    return df


def coerce_types(df):
    """Order Date -> date, Order Time -> time, quantities -> int.

    Parsing is strict so an unexpected format fails the run (and triggers the
    failure email) rather than silently loading blanks.
    """
    df["Order Date"] = pd.to_datetime(df["Order Date"], format="mixed").dt.date
    df["Order Time"] = pd.to_datetime(df["Order Time"], format="mixed").dt.time
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round().astype("Int64")
    return df


def validate_keys(df, source_name):
    """Fail if any order line is missing a key or a key appears more than once.

    (Sales Order #, Order Line #) is the primary key of both the staging and the
    target table, and every export is expected to honour it. A violation raises,
    so the hourly run fails before touching the database and the runner logs
    FAIL to ETL health and sends the failure email with this message in the log.
    """
    missing_key = df[KEY_COLUMNS].isna().any(axis=1)
    if missing_key.any():
        raise ValueError(
            f"{source_name}: {missing_key.sum()} row(s) are missing a primary-key "
            f"value {KEY_COLUMNS} (file row(s) {(df.index[missing_key] + 2).tolist()[:10]})."
        )
    dup = df.duplicated(subset=KEY_COLUMNS, keep=False)
    if dup.any():
        dup_keys = df.loc[dup, KEY_COLUMNS].drop_duplicates()
        examples = [tuple(k) for k in dup_keys.head(10).itertuples(index=False)]
        raise ValueError(
            f"{source_name}: primary key {KEY_COLUMNS} is not unique — {len(dup_keys)} "
            f"key(s) repeated across {dup.sum()} rows, e.g. {examples}."
        )
    print(f"Primary-key check passed — {len(df)} unique {KEY_COLUMNS}.")


def prepare(file_path):
    """Read, validate and cleanse ONE export into the load shape."""
    df = read_fillkill_file(file_path)
    validate_keys(df, os.path.basename(file_path))
    df = add_po_columns(df)
    df = coerce_types(df)
    return df[LOAD_COLUMNS]
