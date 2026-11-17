"""Ingestion & cleansing for Medline's Order Detail (orders & confirms timing) feed.

The source is Medline's "MHS - ORDER DETAIL REPORT-<yyyy-mm-dd-hh-mm-ss>.xls"
export ("Order Detail with Confirm and ASN Timing" in its footer). One sheet:

    header      43 columns (SOURCE_COLUMNS)
    data        one row per order line: (Order #, Order Line) is unique
    blank row
    totals      "Grand Totals", then "# of PO's:", "# of Medline Orders:",
                "# of Order Lines:", "# of Bad / Rejected Lines:", "EDI 850 PO
                Count", "EDI 855 Confirm Count", "EDI 856 ASN Count" — label in
                Sold-To Name, value in Ship-To #, every other cell blank
    blank row
    footer      "Dates & Times Time Zone:", the report title,
                "Date Range:mm/dd/yyyy to mm/dd/yyyy" and the run date — first
                cell only

Each file is a day-to-date snapshot: the Date Range is a single day, which
becomes ``Report Date``, and a later file the same day repeats the earlier
lines plus the new ones (see persistence.py for the day-replace load).

Checks that fail the run before anything is written: a missing column, no
single-day Date Range, a data row missing Order # / Order Line or repeating
them, an unparseable date / time / number, and a "# of Order Lines" total that
disagrees with the rows read (a truncated attachment). The other totals are
compared too, but a mismatch there is only a warning.

Source column names are kept as-is (whitespace-stripped, so typos such as
"EDI Date Re" and the double space in "Mins  to Order Create" stay).
``Order Line`` is loaded without its leading zeros ("000010" -> "10", still
text), as in the Fill-Kill feed (since v1.1). Derived columns (see
src/medline_keys.py), then Report Date and source file, follow:

- ``PO``     — the leading 10 digits of ``PO#`` ("3000571298-STPO" -> "3000571298")
- ``POLine`` — ``PO Line #`` with leading zeros removed ("000001" -> "1")
- ``ShipTo`` — ``Ship-To #`` with leading zeros removed ("0002522416" -> "2522416")
- ``SoldTo`` — ``Sold-To #`` with leading zeros removed ("0002159659" -> "2159659")
"""

import os
import re
from datetime import datetime

import pandas as pd

from src import medline_keys

LINE_KEY = ["Order #", "Order Line"]
KEY_COLUMNS = ["Report Date"] + LINE_KEY

SOURCE_COLUMNS = [
    "Order Create Date", "Sold-To #", "Sold-To Name", "Ship-To #", "Ship-To Name",
    "Order #", "Channel", "PO#", "Buyer Name", "Cust Item #", "PO Line #",
    "Item # on PO", "Medline Item #", "Medline Category", "Mfr Item #",
    "Manufacturer", "Item Description", "Order Qty", "Open Qty", "Shipped Qty",
    "UM", "Bad Line", "Rej Code", "Line Type", "EDI Date Re", "EDI Data Recv Time",
    "Order Create Time", "855 Date", "855 Time", "Mins  to Order Create",
    "Mins to Confirm", "SAP Delv Doc #", "SAP Delv Doc Create Date",
    "SAP Delv Doc Create Time", "ASN Creation Date", "ASN Creation Time",
    "Branch", "Order Line", "Lot", "Expiration", "PO Type", "SCAC", "SortCode",
]
DERIVED_COLUMNS = ["PO", "POLine", "ShipTo", "SoldTo"]
REPORT_DATE_COLUMN = "Report Date"
SOURCE_FILE_COLUMN = "source file"

# Column order shared by the staging and target tables (see persistence.py).
LOAD_COLUMNS = SOURCE_COLUMNS + DERIVED_COLUMNS + [REPORT_DATE_COLUMN, SOURCE_FILE_COLUMN]

# Real Excel dates/times, except that Medline writes "SAP Delv Doc Create Time"
# and "ASN Creation Time" as text ("7:30:18AM") and "ASN Creation Date" as
# "10/07/2026" — format="mixed" parses each cell on its own.
DATE_COLUMNS = [
    "Order Create Date", "EDI Date Re", "855 Date", "SAP Delv Doc Create Date",
    "ASN Creation Date", "Expiration",
]
TIME_COLUMNS = [
    "EDI Data Recv Time", "Order Create Time", "855 Time",
    "SAP Delv Doc Create Time", "ASN Creation Time",
]
INT_COLUMNS = [
    "Order Qty", "Open Qty", "Shipped Qty", "Mins  to Order Create", "Mins to Confirm",
]

# The totals block: label in TOTALS_LABEL_COLUMN, value in TOTALS_VALUE_COLUMN.
TOTALS_LABEL_COLUMN = "Sold-To Name"
TOTALS_VALUE_COLUMN = "Ship-To #"
ORDER_LINES_TOTAL = "# of Order Lines"

DATE_RANGE_PATTERN = re.compile(
    r"Date Range:\s*(\d{1,2}/\d{1,2}/\d{4})\s*to\s*(\d{1,2}/\d{1,2}/\d{4})"
)


def read_order_detail_file(file_path):
    """Read one export: strip names/values, validate, split off totals and footer.

    Returns (rows, totals, footer).
    """
    df = pd.read_excel(file_path, engine="calamine", dtype=str)
    df.columns = [str(col).strip() for col in df.columns]
    validate_columns(df, SOURCE_COLUMNS)
    df = df[SOURCE_COLUMNS].copy()
    for col in SOURCE_COLUMNS:
        df[col] = df[col].str.strip().replace("", pd.NA)
    return split_trailer(df)


def validate_columns(df, required_columns):
    """Raise ValueError if any *required_columns* are missing from *df*."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Validation Failed. Missing columns: {missing}")
    print("Column validation successful.")
    return True


def split_trailer(df):
    """Split the data rows from the totals block and the footer, by shape.

    - blank: every cell empty (the spacer rows) — dropped
    - footer: only the first cell filled
    - totals: only the totals label (and its value) filled

    No order line looks like either (every line has an Order #), so a data row
    with a missing key is kept and fails ``validate_keys`` loudly instead.
    """
    filled = df.notna()
    blank = ~filled.any(axis=1)
    footer = filled[SOURCE_COLUMNS[0]] & ~filled[SOURCE_COLUMNS[1:]].any(axis=1)
    others = [c for c in SOURCE_COLUMNS if c not in (TOTALS_LABEL_COLUMN, TOTALS_VALUE_COLUMN)]
    totals = filled[TOTALS_LABEL_COLUMN] & ~filled[others].any(axis=1)
    data = ~(blank | footer | totals)
    return df[data].reset_index(drop=True), df[totals], df[footer]


def parse_report_date(footer, source_name):
    """Return the single day of the footer's "Date Range: mm/dd/yyyy to mm/dd/yyyy"."""
    for text in footer[SOURCE_COLUMNS[0]].dropna():
        match = DATE_RANGE_PATTERN.search(text)
        if match:
            start, end = (datetime.strptime(d, "%m/%d/%Y").date() for d in match.groups())
            if start != end:
                raise ValueError(
                    f"{source_name}: Date Range {start} to {end} spans more than one day; "
                    "the day-replace load expects a single day."
                )
            return start
    raise ValueError(f"{source_name}: no 'Date Range: mm/dd/yyyy to mm/dd/yyyy' line in the footer.")


def parse_totals(totals):
    """{label without trailing colon: int} for the totals rows that carry a value.

    A label with a blank value (the zero-line file leaves the counts blank)
    counts as 0; "Grand Totals" is a heading and is skipped.
    """
    parsed = {}
    for label, value in zip(totals[TOTALS_LABEL_COLUMN], totals[TOTALS_VALUE_COLUMN]):
        label = label.rstrip(":").strip()
        if label.lower() == "grand totals":
            continue
        parsed[label] = 0 if pd.isna(value) else int(float(value))
    return parsed


def check_totals(df, totals, source_name):
    """Compare Medline's totals with the rows read; return warning strings.

    "# of Order Lines" must equal the row count, else the attachment is cut
    short or the layout moved — raise. The other totals are checked against
    what they appear to count (verified on the 2026-10-07 sample) and only
    warn, since their exact definition is Medline's.
    """
    expected = {
        ORDER_LINES_TOTAL: len(df),
        "# of PO's": df["PO#"].nunique(),
        "# of Medline Orders": df["Order #"].nunique(),
        "# of Bad / Rejected Lines": int(df["Bad Line"].eq("YES").sum()),
    }
    warnings = []
    for label, actual in expected.items():
        if label not in totals:
            warnings.append(f"Warning: {source_name} has no '{label}' total to check")
        elif totals[label] != actual:
            if label == ORDER_LINES_TOTAL:
                raise ValueError(
                    f"{source_name}: the file says {totals[label]} order lines but "
                    f"{actual} were read — truncated attachment or changed layout?"
                )
            warnings.append(
                f"Warning: {source_name} total '{label}' is {totals[label]}, rows give {actual}"
            )
    for w in warnings:
        print(w)
    if not warnings:
        print(f"Totals check passed — {len(df)} order lines.")
    return warnings


def validate_keys(df, source_name):
    """Fail if any order line is missing Order # / Order Line or repeats them."""
    missing_key = df[LINE_KEY].isna().any(axis=1)
    if missing_key.any():
        raise ValueError(
            f"{source_name}: {missing_key.sum()} row(s) are missing a key value {LINE_KEY} "
            f"(file row(s) {(df.index[missing_key] + 2).tolist()[:10]})."
        )
    dup = df.duplicated(subset=LINE_KEY, keep=False)
    if dup.any():
        dup_keys = df.loc[dup, LINE_KEY].drop_duplicates()
        examples = [tuple(k) for k in dup_keys.head(10).itertuples(index=False)]
        raise ValueError(
            f"{source_name}: key {LINE_KEY} is not unique — {len(dup_keys)} key(s) "
            f"repeated across {dup.sum()} rows, e.g. {examples}."
        )
    print(f"Key check passed — {len(df)} unique {LINE_KEY}.")


def add_derived_columns(df):
    """Append PO and POLine (from 'PO#' / 'PO Line #'), ShipTo and SoldTo."""
    df = medline_keys.add_po_columns(df, "PO#", "PO Line #")
    df = medline_keys.add_ship_to_column(df, "Ship-To #")
    return medline_keys.add_sold_to_column(df, "Sold-To #")


def coerce_types(df):
    """Dates -> date, times -> time, quantities and minutes -> int.

    Parsing is strict so an unexpected format fails the run (and triggers the
    failure email) rather than silently loading blanks.
    """
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="mixed").dt.date
    for col in TIME_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="mixed").dt.time
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round().astype("Int64")
    return df


def prepare(file_path):
    """Read, check and cleanse ONE export into the load shape.

    Returns (rows, report_date, warnings). *report_date* comes from the footer,
    so it is known even for a file with no order lines.
    """
    source_name = os.path.basename(file_path)
    df, totals, footer = read_order_detail_file(file_path)
    report_date = parse_report_date(footer, source_name)
    print(f"Read {len(df)} order line(s) from {source_name} (Report Date {report_date}).")
    warnings = check_totals(df, parse_totals(totals), source_name)
    df = medline_keys.strip_order_line(df, "Order Line")
    validate_keys(df, source_name)
    df = add_derived_columns(df)
    df = coerce_types(df)
    df[REPORT_DATE_COLUMN] = report_date
    df[SOURCE_FILE_COLUMN] = source_name
    return df[LOAD_COLUMNS], report_date, warnings
