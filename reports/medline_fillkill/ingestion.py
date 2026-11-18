"""Ingestion & cleansing for the Medline Fill-Kill feed.

The source is Medline's "MHS - FILL KILL REPORT-<yyyy-mm-dd-hh-mm-ss>.xlsx"
export (footer title "Medline Insight: LUM Fill-Kill Report v3 - SAP Data
Direct"): one header row, the order lines, then a 3-line footer whose first
cell carries the report title / date range / run date and whose other cells
are blank. Those footer rows are dropped by shape (see ``remove_footer``).
Each file is a day-to-date snapshot (single-day Date Range): a later file the
same day repeats the earlier lines, with their latest values, plus new ones.

The primary key is (Sales Order #, Order Line #). A row missing either value
fails the run before anything is written. A line listed more than once (several
shipments of one line?) is not expected: since v1.4 the first row in file order
is kept, the others dropped, and a warning that is also emailed
(``Prepared.alerts``, to notification.warning_recipients) names the lines and
the columns that differ.

Source column names are kept as-is (whitespace-stripped), with one exception:
v3's "Customer Service Comments (SAP order line level Internal Item History
Text - Z106)" is loaded into the existing "Internal Order Comments from SAP
(Z106)" column — the same SAP Z106 text under the name it had in the earlier
"WPH - FILL KILL REPORT" export (see ``SOURCE_RENAMES``). That export's
``Plant`` column is gone from v3; it stays in the table, blank for v3 rows.

If the layout changes, the outcome depends on the kind of change:

- an expected column is missing (or renamed) -> the run fails before anything
  is written, and the error lists the missing and the unexpected columns;
- an extra column appears -> the run still loads the expected columns, and a
  "Warning: ... not loaded" goes to the log and to the ETL-health Error column
  of the SUCCESS row (no email). Loading it needs a code change (SOURCE_COLUMNS +
  persistence.COLUMN_TYPES; ensure_tables then adds it to the tables);
- columns in a new order, or headers with extra spaces -> no effect.

``Order Line #`` is loaded without its leading zeros ("000010" -> "10", still
text), as in the Order Timing feed (since v1.3).

Four derived columns are appended (see src/medline_keys.py):

- ``PO``     — the leading 10 digits of ``PO #`` ("2600301531-STPO" -> "2600301531")
- ``POLine`` — ``PO Line`` with leading zeros removed ("000003" -> "3")
- ``ShipTo`` — ``Ship-To`` with leading zeros removed ("0002464430" -> "2464430")
- ``SoldTo`` — ``Sold-To`` with leading zeros removed ("0002159659" -> "2159659")
"""

import os
from collections import namedtuple

import pandas as pd

from src import medline_keys

KEY_COLUMNS = ["Sales Order #", "Order Line #"]

# File column name -> table column name, applied right after reading.
SOURCE_RENAMES = {
    "Customer Service Comments (SAP order line level Internal Item History Text - Z106)":
        "Internal Order Comments from SAP (Z106)",
}

# The v3 layout, in file order, after SOURCE_RENAMES.
SOURCE_COLUMNS = [
    "Sold-To", "Sold-To Name", "Ship-To", "Ship-To Name", "Ship-To Name 2",
    "Order Date", "Order Time", "PO #", "PO Line", "Sales Order #",
    "Order Line #", "Item", "Item Description", "Item # on PO",
    "Customer Item #", "Mfr. Item #", "Manufacturer Name", "Order Qty", "UM",
    "Filled Qty", "Buyer Name",
    "Internal Order Comments from SAP (Z106)",
    "Product Allocation Qty", "Open Product Allocation Qty", "PA Fulfill Flag",
    "Possible Subst. Item", "Customer Preferred Substitute Item",
    "DED Qty", "DED Valid From Date", "Forecast Qty", "30 Day Delivered Qty",
    "Est Avail Date", "ReasonTypeText", "MBO Type Description",
    "Allocation Period Type", "Allocation Level", "DED UOM Code",
]
DERIVED_COLUMNS = ["PO", "POLine", "ShipTo", "SoldTo"]
SOURCE_FILE_COLUMN = "source file"

# Column order shared by the staging and target tables (see persistence.py).
LOAD_COLUMNS = SOURCE_COLUMNS + DERIVED_COLUMNS + [SOURCE_FILE_COLUMN]

DATE_COLUMNS = ["Order Date", "DED Valid From Date", "Est Avail Date"]
INT_COLUMNS = [
    "Order Qty", "Filled Qty", "Product Allocation Qty",
    "Open Product Allocation Qty", "DED Qty", "30 Day Delivered Qty",
]
DECIMAL_COLUMNS = ["Forecast Qty"]   # 3 decimals in the source

# What prepare() returns. *alerts* are the warnings (also in *warnings*) that
# are emailed to notification.warning_recipients.
Prepared = namedtuple("Prepared", "rows warnings alerts")


def read_fillkill_file(file_path):
    """Read one export: strip names/values, validate, trim the footer, tag the file.

    Returns (rows, warnings); *warnings* names any column outside the expected
    layout, which is not loaded.
    """
    source_name = os.path.basename(file_path)
    df = pd.read_excel(file_path, engine="calamine", dtype=str)
    df.columns = [str(col).strip() for col in df.columns]
    df = df.rename(columns=SOURCE_RENAMES)
    extra = unexpected_columns(df)
    validate_columns(df, SOURCE_COLUMNS, extra)
    warnings = []
    if extra:
        warnings.append(
            f"Warning: {source_name} has {len(extra)} column(s) outside the expected "
            f"layout, not loaded: {extra}"
        )
        print(warnings[-1])
    df = df[SOURCE_COLUMNS].copy()
    for col in SOURCE_COLUMNS:
        df[col] = df[col].str.strip().replace("", pd.NA)
    df = remove_footer(df)
    df[SOURCE_FILE_COLUMN] = source_name
    print(f"Read {len(df)} rows from {source_name}")
    return df, warnings


def unexpected_columns(df):
    """Columns of *df* outside SOURCE_COLUMNS.

    A blank-header column with no values (pandas names it "Unnamed: <n>") is
    just empty formatted cells, not a new column, so it is ignored.
    """
    return [
        col for col in df.columns
        if col not in SOURCE_COLUMNS
        and not (col.startswith("Unnamed:") and df[col].isna().all())
    ]


def validate_columns(df, required_columns, unexpected=()):
    """Raise ValueError if any *required_columns* are missing from *df*.

    *unexpected* (the file's columns outside the expected layout) is added to
    the message, so a renamed column shows up as a missing / unexpected pair.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        detail = f"Missing columns: {missing}"
        if unexpected:
            detail += f"; unexpected columns (renamed?): {list(unexpected)}"
        raise ValueError(f"Validation Failed. {detail}")
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


def add_derived_columns(df):
    """Append PO and POLine (from 'PO #' / 'PO Line'), ShipTo and SoldTo."""
    df = medline_keys.add_po_columns(df, "PO #", "PO Line")
    df = medline_keys.add_ship_to_column(df, "Ship-To")
    return medline_keys.add_sold_to_column(df, "Sold-To")


def coerce_types(df):
    """Dates -> date, Order Time -> time, quantities -> int, Forecast Qty -> decimal.

    Parsing is strict so an unexpected format fails the run (and triggers the
    failure email) rather than silently loading blanks.
    """
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="mixed").dt.date
    df["Order Time"] = pd.to_datetime(df["Order Time"], format="mixed").dt.time
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round().astype("Int64")
    for col in DECIMAL_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round(3)
    return df


def validate_keys(df, source_name):
    """Fail if any order line is missing a primary-key value.

    (Sales Order #, Order Line #) is the primary key of both the staging and the
    target table, so such a row cannot be loaded. It raises, so the hourly run
    fails before touching the database and the runner logs FAIL to ETL health
    and sends the failure email with this message in the log.
    """
    missing_key = df[KEY_COLUMNS].isna().any(axis=1)
    if missing_key.any():
        raise ValueError(
            f"{source_name}: {missing_key.sum()} row(s) are missing a primary-key "
            f"value {KEY_COLUMNS} (file row(s) {(df.index[missing_key] + 2).tolist()[:10]})."
        )


def drop_repeated_keys(df, source_name):
    """Keep the first row of an order line listed more than once; return (rows, alerts).

    Each line is expected once per export. A repeat is unexpected but not
    fatal: the first row in file order is kept, the others dropped, and the
    alert (a "Warning: ..." emailed to notification.warning_recipients) names
    the lines and the columns whose values differ between the repeats.
    """
    dup = df.duplicated(subset=KEY_COLUMNS, keep=False)
    if not dup.any():
        print(f"Primary-key check passed — {len(df)} unique {KEY_COLUMNS}.")
        return df, []
    repeats = df[dup]
    dup_keys = repeats[KEY_COLUMNS].drop_duplicates()
    examples = [tuple(k) for k in dup_keys.head(5).itertuples(index=False)]
    by_key = repeats.groupby(KEY_COLUMNS)
    differing = [
        col for col in df.columns
        if col not in KEY_COLUMNS and by_key[col].nunique(dropna=False).gt(1).any()
    ]
    kept = df[~df.duplicated(subset=KEY_COLUMNS)].reset_index(drop=True)
    alert = (
        f"Warning: {source_name} lists {len(dup_keys)} order line(s) {KEY_COLUMNS} more "
        f"than once ({dup.sum()} rows); kept the first row of each, dropped "
        f"{len(df) - len(kept)}. e.g. {examples}; columns that differ: "
        f"{differing or 'none (identical rows)'}"
    )
    print(alert)
    return kept, [alert]


def prepare(file_path):
    """Read, validate and cleanse ONE export into the load shape.

    Returns a ``Prepared``: *rows* in LOAD_COLUMNS, *warnings* (extra columns,
    see ``read_fillkill_file``, plus the alerts) and *alerts* (repeated lines,
    see ``drop_repeated_keys``).
    """
    source_name = os.path.basename(file_path)
    df, warnings = read_fillkill_file(file_path)
    df = medline_keys.strip_order_line(df, "Order Line #")
    validate_keys(df, source_name)
    df, alerts = drop_repeated_keys(df, source_name)
    df = add_derived_columns(df)
    df = coerce_types(df)
    return Prepared(df[LOAD_COLUMNS], warnings + alerts, alerts)
